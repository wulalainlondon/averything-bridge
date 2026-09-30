package remotedesktop

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// VideoService owns at most one receive-only WebRTC video peer. ICE host
// candidates are restricted to the Mac's Tailscale IPv4; no STUN, TURN, public
// Bridge route, microphone, or recording is involved.
type VideoService struct {
	hostIP  net.IP
	peerIP  net.IP
	helper  string
	dataDir string
	mu      sync.Mutex
	pc      *webrtc.PeerConnection
	cancel  context.CancelFunc
}

func NewVideoService(hostIP, peerIP, helperPath, dataDir string) (*VideoService, error) {
	ip := net.ParseIP(hostIP).To4()
	peer := net.ParseIP(peerIP).To4()
	if ip == nil || ip[0] != 100 || ip[1] < 64 || ip[1] > 127 ||
		peer == nil || peer[0] != 100 || peer[1] < 64 || peer[1] > 127 || ip.Equal(peer) {
		return nil, errors.New("video requires distinct Tailscale IPv4 host and peer")
	}
	if info, err := os.Stat(helperPath); err != nil || info.IsDir() || info.Mode()&0111 == 0 {
		return nil, errors.New("video helper missing or not executable")
	}
	if info, err := os.Stat(dataDir); err != nil || !info.IsDir() {
		return nil, errors.New("video data directory missing")
	}
	return &VideoService{hostIP: ip, peerIP: peer, helper: helperPath, dataDir: dataDir}, nil
}

func retainPeerCandidates(sdp string, peerIP net.IP) string {
	var retained strings.Builder
	for _, line := range strings.SplitAfter(sdp, "\n") {
		if strings.HasPrefix(line, "a=candidate:") {
			fields := strings.Fields(line)
			if len(fields) < 5 || !net.ParseIP(fields[4]).Equal(peerIP) {
				continue
			}
		}
		retained.WriteString(line)
	}
	return retained.String()
}

func (v *VideoService) Offer(sessionCtx context.Context, sdp string) (string, error) {
	if sessionCtx.Err() != nil || len(sdp) == 0 || len(sdp) > 1<<20 {
		return "", errors.New("invalid or expired video offer")
	}
	sdp = retainPeerCandidates(sdp, v.peerIP)
	var setting webrtc.SettingEngine
	setting.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	setting.SetIPFilter(func(ip net.IP) bool { return ip.Equal(v.hostIP) })
	pc, err := webrtc.NewAPI(webrtc.WithSettingEngine(setting)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return "", err
	}
	streamCtx, cancel := context.WithCancel(sessionCtx)
	closeOnError := true
	defer func() {
		if closeOnError {
			cancel()
			_ = pc.Close()
		}
	}()
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("[remote-desktop] video peer state=%s", state)
		if state == webrtc.PeerConnectionStateConnected {
			go func() {
				time.Sleep(time.Second)
				stats := pc.GetStats()
				for _, stat := range stats {
					pair, ok := stat.(webrtc.ICECandidatePairStats)
					if !ok || !pair.Nominated {
						continue
					}
					local, localOK := stats[pair.LocalCandidateID].(webrtc.ICECandidateStats)
					remote, remoteOK := stats[pair.RemoteCandidateID].(webrtc.ICECandidateStats)
					if localOK && remoteOK {
						log.Printf("[remote-desktop] selected ICE pair %s -> %s", local.IP, remote.IP)
						if !net.ParseIP(local.IP).Equal(v.hostIP) || !net.ParseIP(remote.IP).Equal(v.peerIP) {
							log.Printf("[remote-desktop] closing non-Tailscale ICE pair")
							cancel()
							_ = pc.Close()
						}
					}
				}
			}()
		}
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateDisconnected || state == webrtc.PeerConnectionStateClosed {
			cancel()
		}
	})
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264}, "desktop", "bridge-remote")
	if err != nil {
		return "", err
	}
	sender, err := pc.AddTrack(track)
	if err != nil {
		return "", err
	}
	go func() {
		buffer := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(buffer); err != nil {
				return
			}
		}
	}()
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}); err != nil {
		return "", err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return "", err
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		return "", err
	}
	select {
	case <-gathered:
	case <-time.After(5 * time.Second):
		return "", errors.New("Tailscale ICE gathering timed out")
	case <-sessionCtx.Done():
		return "", sessionCtx.Err()
	}
	local := pc.LocalDescription()
	if local == nil || !containsHostCandidate(local.SDP, v.hostIP.String()) {
		return "", errors.New("Tailscale ICE candidate unavailable")
	}
	v.mu.Lock()
	v.closeLocked()
	v.pc, v.cancel = pc, cancel
	v.mu.Unlock()
	closeOnError = false
	go v.run(streamCtx, pc, track)
	return local.SDP, nil
}

func containsHostCandidate(sdp, ip string) bool {
	for _, line := range strings.Split(sdp, "\n") {
		if strings.HasPrefix(line, "a=candidate:") && strings.Contains(line, " "+ip+" ") {
			return true
		}
	}
	return false
}

func (v *VideoService) run(ctx context.Context, pc *webrtc.PeerConnection, track *webrtc.TrackLocalStaticSample) {
	defer func() {
		log.Printf("[remote-desktop] video worker exited: context=%v", ctx.Err())
		_ = pc.Close()
		v.mu.Lock()
		if v.pc == pc {
			v.pc, v.cancel = nil, nil
		}
		v.mu.Unlock()
	}()
	reader, cleanup, err := openStream(ctx, v.helper, v.dataDir)
	if err != nil {
		log.Printf("[remote-desktop] video helper launch: %v", err)
		return
	}
	defer cleanup()
	frames := make(chan []byte, 1)
	readErr := make(chan error, 1)
	go readLatestFrames(ctx, reader, frames, readErr)
	lastSent := time.Now()
	sent := 0
	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-frames:
			if !ok {
				if err := <-readErr; err != nil && ctx.Err() == nil {
					log.Printf("[remote-desktop] video stream ended after %d sent frames: %v", sent, err)
				}
				return
			}
			now := time.Now()
			duration := now.Sub(lastSent)
			if duration < time.Millisecond {
				duration = 100 * time.Millisecond
			}
			lastSent = now
			if err := track.WriteSample(media.Sample{Data: frame, Duration: duration}); err != nil {
				log.Printf("[remote-desktop] video write after %d frames: %v", sent, err)
				return
			}
			sent++
		}
	}
}

// The capture socket must be drained independently of network writes. A slow
// mobile path drops stale frames instead of blocking ScreenCaptureKit's output.
func readLatestFrames(ctx context.Context, reader io.Reader, frames chan []byte, readErr chan error) {
	defer close(frames)
	defer close(readErr)
	var err error
	defer func() { readErr <- err }()
	for ctx.Err() == nil {
		var header [4]byte
		if _, err = io.ReadFull(reader, header[:]); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(header[:])
		if length == 0 || length > 4_000_000 {
			err = errors.New("invalid video access unit length")
			return
		}
		frame := make([]byte, length)
		if _, err = io.ReadFull(reader, frame); err != nil {
			return
		}
		offerLatestFrame(frames, frame)
	}
}

func offerLatestFrame(frames chan []byte, frame []byte) {
	select {
	case frames <- frame:
		return
	default:
	}
	select {
	case <-frames:
	default:
	}
	select {
	case frames <- frame:
	default:
	}
}

func (v *VideoService) Close() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.closeLocked()
}

func (v *VideoService) closeLocked() {
	if v.cancel != nil {
		v.cancel()
		v.cancel = nil
	}
	if v.pc != nil {
		_ = v.pc.Close()
		v.pc = nil
	}
}
