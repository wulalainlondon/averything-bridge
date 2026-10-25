package remotedesktop

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
)

func TestReadLatestFramesDropsStaleFrames(t *testing.T) {
	var stream bytes.Buffer
	for _, value := range []byte{1, 2, 3} {
		if err := binary.Write(&stream, binary.BigEndian, uint32(1)); err != nil {
			t.Fatal(err)
		}
		stream.WriteByte(value)
	}
	frames := make(chan []byte, 1)
	readErr := make(chan error, 1)
	readLatestFrames(context.Background(), &stream, frames, readErr)
	frame, ok := <-frames
	if !ok || len(frame) != 1 || frame[0] != 3 {
		t.Fatalf("latest frame = %v, open=%v", frame, ok)
	}
	if _, ok := <-frames; ok {
		t.Fatal("frame channel not closed")
	}
	if err := <-readErr; !errors.Is(err, io.EOF) {
		t.Fatalf("read error = %v", err)
	}
}

func TestRetainPeerCandidates(t *testing.T) {
	sdp := "v=0\r\n" +
		"a=candidate:1 1 udp 1 192.168.68.50 5555 typ host\r\n" +
		"a=candidate:2 1 udp 1 100.90.142.35 5556 typ host\r\n" +
		"a=candidate:3 1 udp 1 device.local 5557 typ host\r\n" +
		"a=end-of-candidates\r\n"
	filtered := retainPeerCandidates(sdp, net.ParseIP("100.90.142.35"))
	if strings.Contains(filtered, "192.168.68.50") || strings.Contains(filtered, "device.local") {
		t.Fatalf("non-peer candidates retained: %q", filtered)
	}
	if !strings.Contains(filtered, "100.90.142.35") || !strings.Contains(filtered, "a=end-of-candidates") {
		t.Fatalf("required SDP fields lost: %q", filtered)
	}
}
