package linevoice

import (
	"context"
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

func phone(t *testing.T) (*webrtc.PeerConnection, *webrtc.TrackLocalStaticSample, string) {
	t.Helper()
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	track, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "phone", "phone")
	if err != nil {
		t.Fatal(err)
	}
	sender, err := pc.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		b := make([]byte, 1500)
		for {
			if _, _, e := sender.Read(b); e != nil {
				return
			}
		}
	}()
	if _, err = pc.CreateDataChannel("oai-events", nil); err != nil {
		t.Fatal(err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	done := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("phone gather timeout")
	}
	return pc, track, pc.LocalDescription().SDP
}
func TestActualWebRTCDuplexThroughIsolatedOpusWorker(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	worker, err := filepath.Abs("../../../tools/linejs-voice/loopback-fixture.mjs")
	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(worker); os.IsNotExist(statErr) {
		worker, err = filepath.Abs("../../services/line-voice/loopback-fixture.mjs")
		if err != nil {
			t.Fatal(err)
		}
	}
	codec := exec.Command(node, "--input-type=module", "-e", `import{opusCodecFactory}from'@evex/linejs/call';const f=await opusCodecFactory(),e=f.newEncoder({sampleRate:48000,channels:1,frameDurationMs:20});const p=e.encode({sampleRate:48000,channels:1,samples:Int16Array.from({length:960},(_,i)=>Math.round(8000*Math.sin(i/12)))});process.stdout.write(Buffer.from(p).toString('base64'));e.close();`)
	codec.Dir = filepath.Dir(worker)
	raw, err := codec.Output()
	if err != nil {
		t.Fatal(err)
	}
	packet, err := base64.StdEncoding.DecodeString(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	pc, track, offer := phone(t)
	received := make(chan []byte, 1)
	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		for i := 0; i < 5; i++ {
			p, _, e := remote.ReadRTP()
			if e != nil {
				return
			}
			if i == 4 {
				received <- p.Payload
			}
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{}, 1)
	var closed atomic.Int32
	call, answer, err := Offer(ctx, Config{Node: node, Worker: worker}, "line_"+strings.Repeat("a", 64), "offline fixture", offer, func(e Event) {
		if e.State == "connected" {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
		if e.State == "closed" {
			closed.Add(1)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(8 * time.Second):
		t.Fatal("fixture worker never became ready")
	}
	sendCtx, stopSend := context.WithCancel(ctx)
	defer stopSend()
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-sendCtx.Done():
				return
			case <-tick.C:
				_ = track.WriteSample(media.Sample{Data: packet, Duration: 20 * time.Millisecond})
			}
		}
	}()
	select {
	case p := <-received:
		if len(p) == 0 {
			t.Fatal("empty return audio")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no return audio across real WebRTC and Node codec path")
	}
	cancel()
	deadline, stop := context.WithTimeout(context.Background(), 6*time.Second)
	defer stop()
	if err = call.WaitClosed(deadline); err != nil {
		t.Fatal(err)
	}
	call.Close()
	time.Sleep(10 * time.Millisecond)
	if closed.Load() != 1 {
		t.Fatalf("closed events=%d", closed.Load())
	}
}
func TestNoWorkerStartsBeforePhoneTransportConnects(t *testing.T) {
	_, _, offer := phone(t)
	marker := filepath.Join(t.TempDir(), "started")
	worker := filepath.Join(t.TempDir(), "worker.mjs")
	if err := os.WriteFile(worker, []byte(`import{writeFileSync}from'node:fs';writeFileSync(process.env.LINE_TEST_MARKER,'started');`), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	call, _, err := Offer(ctx, Config{Worker: worker, Env: append(os.Environ(), "LINE_TEST_MARKER="+marker)}, "test", "fixture", offer, func(Event) {})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	call.Close()
	deadline, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err = call.WaitClosed(deadline); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("worker started before a phone media connection existed")
	}
}
