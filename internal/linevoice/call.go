// Package linevoice terminates the phone WebRTC leg and relays Opus packets to
// an isolated LINEJS process. Only the authenticated Hub may construct a call.
package linevoice

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"
)

type Config struct {
	WorkerProtocol int
	BeforeStart    func(context.Context) error
	Node, Worker   string
	ICE            []webrtc.ICEServer
	Env            []string
	Material       json.RawMessage
}
type Event struct {
	TXFrames, RXFrames            int
	State, Code, Stage, Status    string
	ElapsedMS, DurationMS         int64
	RelCode                       *int
	DroppedPackets, PayloadLength int
}

type Call struct {
	ctx        context.Context
	cancel     context.CancelFunc
	pc         *webrtc.PeerConnection
	track      *webrtc.TrackLocalStaticSample
	cfg        Config
	id, peer   string
	event      func(Event)
	once       sync.Once
	mu         sync.Mutex
	proc       *exec.Cmd
	stdin      io.WriteCloser
	outgoing   chan []byte
	incoming   chan []byte
	connected  chan struct{}
	ready      sync.Once
	done       chan struct{}
	finished   chan struct{}
	mediaReady atomic.Bool
	gotTrack   atomic.Bool
	closing    atomic.Bool
}
type frame struct {
	TXFrames        int             `json:"txFrames,omitempty"`
	RXFrames        int             `json:"rxFrames,omitempty"`
	ProtocolVersion int             `json:"protocol_version,omitempty"`
	Stage           string          `json:"stage,omitempty"`
	Status          string          `json:"status,omitempty"`
	ElapsedMS       int64           `json:"elapsedMs,omitempty"`
	DurationMS      int64           `json:"durationMs,omitempty"`
	RelCode         *int            `json:"relCode,omitempty"`
	DroppedPackets  int             `json:"droppedPackets,omitempty"`
	PayloadLength   int             `json:"payloadLength,omitempty"`
	Type            string          `json:"type"`
	State           string          `json:"state,omitempty"`
	Code            string          `json:"code,omitempty"`
	Data            string          `json:"data,omitempty"`
	Samples         int             `json:"samples,omitempty"`
	CallID          string          `json:"callId,omitempty"`
	PeerName        string          `json:"peerName,omitempty"`
	Material        json.RawMessage `json:"material,omitempty"`
}

func Offer(parent context.Context, cfg Config, id, peer, offer string, onEvent func(Event)) (*Call, string, error) {
	if cfg.Worker == "" || !strings.HasPrefix(offer, "v=0") || len(offer) > 65536 || strings.TrimSpace(peer) == "" || len(peer) > 512 {
		return nil, "", errors.New("invalid_line_voice_request")
	}
	ctx, cancel := context.WithCancel(parent)
	c := &Call{ctx: ctx, cancel: cancel, cfg: cfg, id: id, peer: peer, event: onEvent, outgoing: make(chan []byte, 8), incoming: make(chan []byte, 8), connected: make(chan struct{}), done: make(chan struct{}), finished: make(chan struct{})}
	fail := func(err error) (*Call, string, error) { c.Close(); return nil, "", err }
	engine := &webrtc.MediaEngine{}
	// WebRTC's Opus mapping uses 2 channels even when stereo=0. The worker
	// decodes all negotiated packets to mono before LINE framing.
	if err := engine.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1;stereo=0;sprop-stereo=0"}, PayloadType: 111}, webrtc.RTPCodecTypeAudio); err != nil {
		return fail(err)
	}
	registry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(engine, registry); err != nil {
		return fail(err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(engine), webrtc.WithInterceptorRegistry(registry))
	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: cfg.ICE})
	if err != nil {
		return fail(err)
	}
	c.pc = pc
	c.track, err = webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "line-peer", "line-voice")
	if err != nil {
		return fail(err)
	}
	sender, err := pc.AddTrack(c.track)
	if err != nil {
		return fail(err)
	}
	go func() {
		b := make([]byte, 1500)
		for {
			if _, _, e := sender.Read(b); e != nil {
				return
			}
		}
	}()
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if track.Kind() != webrtc.RTPCodecTypeAudio || !strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeOpus) || c.gotTrack.Swap(true) {
			return
		}
		builder := samplebuilder.New(8, &codecs.OpusPacket{}, 48000)
		for {
			packet, _, e := track.ReadRTP()
			if e != nil {
				return
			}
			if len(packet.Payload) > 4000 {
				continue
			}
			builder.Push(packet)
			for {
				sample := builder.Pop()
				if sample == nil {
					break
				}
				if c.mediaReady.Load() {
					data := append([]byte(nil), sample.Data...)
					select {
					case c.outgoing <- data:
					default:
						select {
						case <-c.outgoing:
						default:
						}
						select {
						case c.outgoing <- data:
						default:
						}
					}
				}
			}
		}
	})
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnMessage(func(webrtc.DataChannelMessage) { /* No AI commands accepted on the LINE audio leg. */ })
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			c.ready.Do(func() { close(c.connected) })
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			go c.Close()
		case webrtc.PeerConnectionStateDisconnected:
			go func() {
				timer := time.NewTimer(10 * time.Second)
				defer timer.Stop()
				select {
				case <-ctx.Done():
				case <-timer.C:
					if pc.ConnectionState() == webrtc.PeerConnectionStateDisconnected {
						c.fail("phone_disconnected")
					}
				}
			}()
		}
	})
	if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		return fail(err)
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return fail(err)
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(answer); err != nil {
		return fail(err)
	}
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fail(ctx.Err())
	case <-timer.C:
		return fail(errors.New("ice_gather_timeout"))
	case <-gather:
	}
	sdp := pc.LocalDescription().SDP
	go func() { <-ctx.Done(); c.Close() }()
	go c.run()
	go c.playout()
	return c, sdp, nil
}

func (c *Call) playout() {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-tick.C:
			select {
			case packet := <-c.incoming:
				if c.mediaReady.Load() {
					if err := c.track.WriteSample(media.Sample{Data: packet, Duration: 20 * time.Millisecond}); err != nil {
						c.fail("phone_audio_write_failed")
						return
					}
				}
			default:
			}
		}
	}
}
func (c *Call) notify(event Event) {
	if c.event != nil {
		c.event(event)
	}
}
func (c *Call) fail(code string) {
	if !c.closing.Load() {
		c.notify(Event{State: "error", Code: code})
	}
	c.Close()
}
func (c *Call) run() {
	defer close(c.done)
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case <-c.ctx.Done():
		return
	case <-timer.C:
		c.fail("phone_connect_timeout")
		return
	case <-c.connected:
	}
	if c.ctx.Err() != nil {
		return
	}
	if c.cfg.BeforeStart != nil {
		if err := c.cfg.BeforeStart(c.ctx); err != nil {
			c.fail("line_call_authorization_changed")
			return
		}
	}
	node := c.cfg.Node
	if node == "" {
		node = "node"
	}
	cmd := exec.Command(node, c.cfg.Worker)
	if c.cfg.Env != nil {
		cmd.Env = c.cfg.Env
	}
	input, err := cmd.StdinPipe()
	if err != nil {
		c.fail("worker_pipe_failed")
		return
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		input.Close()
		c.fail("worker_pipe_failed")
		return
	}
	cmd.Stderr = io.Discard
	c.mu.Lock()
	if c.closing.Load() {
		c.mu.Unlock()
		input.Close()
		output.Close()
		return
	}
	c.proc = cmd
	c.stdin = input
	err = cmd.Start()
	c.mu.Unlock()
	if err != nil {
		c.fail("worker_start_failed")
		return
	}
	workerReady := make(chan struct{})
	var readyOnce sync.Once
	var readyTimer *time.Timer
	if c.cfg.WorkerProtocol == 2 {
		readyTimer = time.AfterFunc(5*time.Second, func() { c.fail("worker_initialization_timeout") })
		defer readyTimer.Stop()
	} else {
		close(workerReady)
	}
	// The single writer owns stdin, including start. Cancellation closes stdin
	// before a late worker can receive a start request.
	go func() {
		encode := json.NewEncoder(input)
		select {
		case <-workerReady:
		case <-c.ctx.Done():
			return
		}
		if c.ctx.Err() != nil {
			return
		}
		if encode.Encode(frame{Type: "start", CallID: c.id, PeerName: c.peer, Material: c.cfg.Material}) != nil {
			c.fail("worker_start_write_failed")
			return
		}
		for {
			select {
			case <-c.ctx.Done():
				return
			case data := <-c.outgoing:
				if encode.Encode(frame{Type: "audio", Data: base64.StdEncoding.EncodeToString(data)}) != nil {
					c.fail("worker_audio_write_failed")
					return
				}
			}
		}
	}()
	lifetime := time.AfterFunc(30*time.Minute, func() { c.fail("call_duration_limit") })
	defer lifetime.Stop()
	scanner := bufio.NewScanner(output)
	scanner.Buffer(make([]byte, 4096), 65536)
	for scanner.Scan() {
		if c.ctx.Err() != nil {
			break
		}
		var f frame
		if json.Unmarshal(scanner.Bytes(), &f) != nil {
			c.fail("worker_protocol_error")
			break
		}
		switch f.Type {
		case "ready":
			if c.cfg.WorkerProtocol != 2 || f.ProtocolVersion != 2 {
				c.fail("worker_protocol_mismatch")
				continue
			}
			readyOnce.Do(func() {
				if readyTimer != nil {
					readyTimer.Stop()
				}
				close(workerReady)
			})
		case "state":
			switch f.State {
			case "starting", "ringing":
				c.notify(Event{State: f.State})
			case "connected":
				c.mediaReady.Store(true)
				c.notify(Event{State: "connected"})
			case "closed":
				c.Close()
			}
		case "error":
			code := f.Code
			switch code {
			case "peer_not_unique", "keychain_unavailable", "existing_login_required", "credential_store_busy", "call_intent_already_used", "call_route_unavailable", "line_media_unavailable":
			default:
				code = "line_call_failed"
			}
			c.fail(code)
		case "diagnostic":
			// Only bounded structural data crosses into durable call telemetry.
			switch f.Code {
			case "line_call_stage", "line_remote_release", "line_audio_packet_dropped", "line_call_duration_limit", "line_media_counts":
				stage := ""
				switch f.Stage {
				case "credentials", "peer", "route", "transport", "invite", "answer", "media", "wrapper", "decode":
					stage = f.Stage
				}
				status := ""
				switch f.Status {
				case "started", "completed", "failed":
					status = f.Status
				}
				if f.ElapsedMS >= 0 && f.ElapsedMS <= 1800000 && f.DurationMS >= 0 && f.DurationMS <= 1800000 && f.PayloadLength >= 0 && f.PayloadLength <= 65536 && f.DroppedPackets >= 0 {
					c.notify(Event{State: "diagnostic", Code: f.Code, Stage: stage, Status: status, ElapsedMS: f.ElapsedMS, DurationMS: f.DurationMS, RelCode: f.RelCode, DroppedPackets: f.DroppedPackets, PayloadLength: f.PayloadLength, TXFrames: f.TXFrames, RXFrames: f.RXFrames})
				}
			}
			continue
		case "audio":
			if c.mediaReady.Load() && f.Samples == 960 && len(f.Data) <= 5400 {
				packet, e := base64.StdEncoding.DecodeString(f.Data)
				if e == nil && len(packet) > 0 && len(packet) <= 4000 {
					select {
					case c.incoming <- packet:
					default:
						select {
						case <-c.incoming:
						default:
						}
						select {
						case c.incoming <- packet:
						default:
						}
					}
				}
			}
		default:
			c.fail("worker_protocol_error")
		}
	}
	err = cmd.Wait()
	if !c.closing.Load() {
		if err != nil || scanner.Err() != nil {
			c.notify(Event{State: "error", Code: "worker_exited"})
		}
		c.Close()
	}
}
func (c *Call) Close() {
	c.once.Do(func() {
		c.closing.Store(true)
		c.mediaReady.Store(false)
		c.cancel()
		c.mu.Lock()
		input, cmd := c.stdin, c.proc
		c.mu.Unlock()
		if input != nil {
			_ = input.Close()
		}
		if c.pc != nil {
			_ = c.pc.Close()
		}
		go func() {
			if cmd != nil && cmd.Process != nil {
				select {
				case <-c.done:
				case <-time.After(4 * time.Second):
					_ = cmd.Process.Kill()
					<-c.done
				}
			}
			close(c.finished)
			c.notify(Event{State: "closed"})
		}()
	})
}
func (c *Call) WaitClosed(ctx context.Context) error {
	select {
	case <-c.finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
