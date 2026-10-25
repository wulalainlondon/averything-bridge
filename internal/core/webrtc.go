package core

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	"everything-go/internal/clientproto"
)

// WebRTC P2P upgrade. The legacy WebSocket (typically Cloudflare-tunneled) acts
// as the signaling channel for SDP/ICE exchange; once a DataChannel opens, the
// bridge promotes it to a full client (serveConn) so the same handshake + route
// loop runs over P2P. The bridge is always the answerer and, like aiortc/Python,
// does NOT trickle its own candidates — it bakes them into the answer SDP after
// ICE gathering completes. The app trickles its candidates via webrtc_ice, which
// the bridge applies. Mirrors bridge/handlers/webrtc_signaling.py.

// stunServers are the public STUN servers used for srflx candidate discovery,
// matching the app's DEFAULT_ICE_SERVERS and the Python bridge's defaults.
var stunServers = []webrtc.ICEServer{
	{URLs: []string{"stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"}},
}

// gatherTimeout caps how long we wait for ICE gathering before sending the
// answer with whatever candidates we have. The app's upgrade timeout is 10s
// (WEBRTC_UPGRADE_TIMEOUT_MS); keep well under it so the answer always lands.
const gatherTimeout = 5 * time.Second

// webrtcPeer is the answering peer connection negotiated over a signaling
// client. promoted flips true once the DataChannel opens and a P2P client takes
// over — after which the PC's lifecycle belongs to the DataChannel (dcConn),
// not the signaling client, so closing the signaling client must not close it.
type webrtcPeer struct {
	pc       *webrtc.PeerConnection
	promoted atomic.Bool
}

// SetICEServers overrides the STUN/TURN servers used when answering offers.
// main wires this from flags/env; tests pass nil to keep negotiation on
// loopback host candidates (no network round-trip).
func (h *Hub) SetICEServers(servers []webrtc.ICEServer) { h.iceServers = servers }

// handleWebRTCOffer answers a client's SDP offer. It runs synchronously on the
// signaling client's read loop so SetRemoteDescription completes before any
// subsequent webrtc_ice frame (read on the same loop) is applied. It blocks
// until ICE gathering finishes (candidates baked into the answer) or the gather
// timeout elapses, then emits webrtc_answer.
func (h *Hub) handleWebRTCOffer(ctx context.Context, c *Client, offer clientproto.WebRTCOfferInput) {
	// Validate on a trimmed copy, but hand Pion the original SDP: the SDP grammar
	// requires every line (including the last) to end in CRLF, so trimming the
	// trailing newline would make the parser fail with EOF.
	if strings.TrimSpace(offer.SDP) == "" {
		c.enqueueEvent(h.client.Error("", "webrtc_offer_invalid", "missing sdp"))
		return
	}
	sdp := offer.SDP

	// Re-offer on the same signaling channel: tear down the previous (un-promoted)
	// PC before negotiating a fresh one.
	if c.rtc != nil && !c.rtc.promoted.Load() {
		_ = c.rtc.pc.Close()
		c.rtc = nil
	}

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: h.iceServers})
	if err != nil {
		log.Printf("[webrtc] NewPeerConnection: %v", err)
		c.enqueueEvent(h.client.Error("", "webrtc_negotiation_failed", "could not create peer connection"))
		return
	}
	peer := &webrtcPeer{pc: pc}
	c.rtc = peer

	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		log.Printf("[webrtc] pc state: %s", s)
		if (s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed) && !peer.promoted.Load() {
			_ = pc.Close()
		}
	})

	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		log.Printf("[webrtc] datachannel offered: label=%s", dc.Label())
		dcc := newDCConn(pc, dc)
		// The promoted DataChannel still downloads over the signaling socket's
		// reachable HTTP origin (often a public tunnel rather than a LAN IP).
		dcc.httpOrigin = c.downloadOrigin
		dc.OnOpen(func() {
			log.Printf("[webrtc] datachannel open — promoting to bridge client")
			peer.promoted.Store(true)
			// Run the full client lifecycle over the DataChannel. The first frame
			// the app sends over the DC is its hello (see useWsWebRtcUpgrade.ts).
			go h.serveConn(context.Background(), dcc)
			// Tell the signaling client the server side is ready (informational;
			// the app's own dc.onopen drives its takeover).
			c.enqueueEvent(h.client.WebRTCReady())
		})
	})

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}); err != nil {
		h.failOffer(c, pc, "setRemoteDescription", err)
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		h.failOffer(c, pc, "createAnswer", err)
		return
	}
	// Begin gathering; GatheringCompletePromise must be obtained before
	// SetLocalDescription so it observes the transition to complete.
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		h.failOffer(c, pc, "setLocalDescription", err)
		return
	}

	select {
	case <-gatherComplete:
	case <-time.After(gatherTimeout):
		log.Printf("[webrtc] ICE gather timeout — answering with partial candidates")
	case <-ctx.Done():
		_ = pc.Close()
		return
	}

	local := pc.LocalDescription()
	if local == nil {
		h.failOffer(c, pc, "localDescription", errors.New("nil after gather"))
		return
	}
	c.enqueueEvent(h.client.WebRTCAnswer(local.SDP))
}

func (h *Hub) failOffer(c *Client, pc *webrtc.PeerConnection, stage string, err error) {
	log.Printf("[webrtc] offer negotiation failed at %s: %v", stage, err)
	_ = pc.Close()
	c.enqueueEvent(h.client.Error("", "webrtc_negotiation_failed", "could not produce SDP answer"))
}

// handleWebRTCICE applies a client-trickled ICE candidate to the answering PC.
// An empty candidate signals end-of-trickle (nothing to apply).
func (h *Hub) handleWebRTCICE(c *Client, ice clientproto.WebRTCICEInput) {
	if c.rtc == nil {
		return
	}
	if strings.TrimSpace(ice.Candidate) == "" {
		return
	}
	init := webrtc.ICECandidateInit{Candidate: ice.Candidate}
	if ice.SDPMid != "" {
		mid := ice.SDPMid
		init.SDPMid = &mid
	}
	if ice.SDPMLineIndex != nil {
		idx := uint16(*ice.SDPMLineIndex)
		init.SDPMLineIndex = &idx
	}
	if err := c.rtc.pc.AddICECandidate(init); err != nil {
		log.Printf("[webrtc] addIceCandidate failed: %v", err)
	}
}

// cleanupWebRTC tears down the answering PC tied to a signaling client when it
// disconnects — unless the DataChannel was promoted, in which case the dcConn
// owns the PC's lifecycle and closing it here would kill a live P2P session.
func (h *Hub) cleanupWebRTC(c *Client) {
	if c.rtc == nil || c.rtc.promoted.Load() {
		return
	}
	_ = c.rtc.pc.Close()
}

// dcConn adapts a Pion DataChannel to wireConn. Pion delivers inbound frames via
// an OnMessage callback, so they are funneled into a buffered inbox the blocking
// Read drains. The consumer (serveConn) starts reading the instant the channel
// opens, so the buffer only absorbs the brief window before that.
type dcConn struct {
	httpOrigin string
	pc         *webrtc.PeerConnection
	dc         *webrtc.DataChannel
	inbox      chan []byte
	done       chan struct{}
	once       sync.Once
}

func newDCConn(pc *webrtc.PeerConnection, dc *webrtc.DataChannel) *dcConn {
	d := &dcConn{
		pc:    pc,
		dc:    dc,
		inbox: make(chan []byte, 512),
		done:  make(chan struct{}),
	}
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		select {
		case d.inbox <- msg.Data:
		default:
			log.Printf("[webrtc] dc inbox overflow, closing")
			d.Close("inbox overflow")
		}
	})
	dc.OnClose(func() { d.Close("datachannel closed") })
	return d
}

func (d *dcConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-d.done:
		return nil, io.EOF
	case b := <-d.inbox:
		return b, nil
	}
}

func (d *dcConn) Write(_ context.Context, data []byte) error {
	select {
	case <-d.done:
		return errors.New("datachannel closed")
	default:
	}
	return d.dc.SendText(string(data))
}

func (d *dcConn) Close(_ string) {
	d.once.Do(func() {
		close(d.done)
		_ = d.dc.Close()
		_ = d.pc.Close()
	})
}

func (d *dcConn) Kind() string { return "webrtc" }

func (d *dcConn) HTTPOrigin() string { return d.httpOrigin }
