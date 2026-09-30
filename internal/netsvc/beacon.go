// Package netsvc implements the bridge's network-presence services: the LAN
// UDP discovery beacon (what the mobile app actually listens for), mDNS
// registration, and the Cloudflare tunnel manager. These mirror the Python
// bridge's discovery_broadcaster.py + network_services.py so the same app
// discovers either bridge identically.
package netsvc

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"syscall"
	"time"
)

// discoveryMagic + the announce shape are the wire contract the app validates
// against (app/src/schemas/discovery.ts DiscoveryAnnounceSchema). Keep byte-for-
// byte parity with discovery_broadcaster.py's payload.
const (
	discoveryMagic    = "CLAUDE_BRIDGE_DISCOVERY_V1"
	beaconVersion     = "1.0"
	defaultDiscoPort  = 8767
	beaconInterval    = 2 * time.Second
	maxBeaconBytes    = 512
	warnRateLimitSecs = 60
)

// announce is the JSON datagram broadcast on the LAN. Field order/keys mirror
// the Python payload; json.Marshal emits compact JSON like separators=(",",":").
type announce struct {
	Magic      string   `json:"magic"`
	Type       string   `json:"type"`
	WSPort     int      `json:"ws_port"`
	IPs        []string `json:"ips"`
	Hostname   string   `json:"hostname"`
	InstanceID string   `json:"instance_id"`
	Version    string   `json:"version"`
	TS         int64    `json:"ts"`
}

// Beacon periodically broadcasts a discover-me datagram so apps on the same
// subnet find this bridge without manual IP entry.
type Beacon struct {
	wsPort     int
	discoPort  int
	instanceID string
	hostname   string

	lastWarn time.Time
}

// NewBeacon builds a beacon advertising wsPort. discoPort is the UDP port the
// app's listener binds (8767); pass 0 for the default.
func NewBeacon(wsPort, discoPort int, instanceID string) *Beacon {
	if discoPort == 0 {
		discoPort = defaultDiscoPort
	}
	host, _ := os.Hostname()
	return &Beacon{wsPort: wsPort, discoPort: discoPort, instanceID: instanceID, hostname: host}
}

// Run broadcasts until ctx is cancelled, re-enumerating interfaces each tick
// (laptops change networks).
func (b *Beacon) Run(ctx context.Context) {
	ips := localIPv4s()
	log.Printf("[discovery] beacon on udp/%d, ws_port=%d, ips=%v, instance=%s",
		b.discoPort, b.wsPort, ips, b.instanceID)

	t := time.NewTicker(beaconInterval)
	defer t.Stop()
	b.sweep(ctx) // immediate first announce
	for {
		select {
		case <-ctx.Done():
			log.Printf("[discovery] beacon stopped")
			return
		case <-t.C:
			b.sweep(ctx)
		}
	}
}

// sweep sends the announce out *each* LAN interface from a socket bound to that
// interface's source IP, then to its directed broadcast (a.b.c.255). Binding to
// the source IP forces egress on the right NIC — on a multi-homed host (LAN +
// Tailscale) an unbound socket sends 255.255.255.255 out the default route only
// and directed broadcasts fail "no route to host". Mirrors _send_v4's intent.
func (b *Beacon) sweep(ctx context.Context) {
	ifaces := lanIPv4Interfaces()

	// The announce advertises every reachable IPv4 (LAN + Tailscale/CGNAT) so the
	// app can pick a route; broadcasting, however, only works on broadcast-capable
	// NICs (a Tailscale utun is point-to-point — 255.255.255.255 there errors
	// "can't assign requested address").
	allIPs := make([]string, 0, len(ifaces))
	for _, ni := range ifaces {
		allIPs = append(allIPs, ni.ip)
	}
	payload := b.payload(allIPs)

	sent := false
	for _, ni := range ifaces {
		if !ni.broadcast {
			continue
		}
		b.sendFromPayload(ctx, ni.ip, []string{ni.bcast, "255.255.255.255"}, payload)
		sent = true
	}
	if !sent {
		// No broadcast-capable NIC — last-resort limited broadcast on the default route.
		b.sendFromPayload(ctx, "0.0.0.0", []string{"255.255.255.255"}, payload)
	}
}

func (b *Beacon) sendFromPayload(ctx context.Context, srcIP string, dsts []string, payload []byte) {
	pc, err := newBroadcastConn(ctx, srcIP)
	if err != nil {
		b.rateLimitedWarn("bind %s failed: %v", srcIP, err)
		return
	}
	defer pc.Close()
	for _, addr := range dsts {
		dst := &net.UDPAddr{IP: net.ParseIP(addr), Port: b.discoPort}
		if _, err := pc.WriteTo(payload, dst); err != nil {
			b.rateLimitedWarn("send %s→%s failed: %v", srcIP, addr, err)
		}
	}
}

func (b *Beacon) payload(ips []string) []byte {
	a := announce{
		Magic:      discoveryMagic,
		Type:       "announce",
		WSPort:     b.wsPort,
		IPs:        ips,
		Hostname:   b.hostname,
		InstanceID: b.instanceID,
		Version:    beaconVersion,
		TS:         time.Now().Unix(),
	}
	if len(a.IPs) == 0 {
		// Schema requires ips.min(1); advertise loopback rather than emit an
		// invalid packet the app would reject.
		a.IPs = []string{"127.0.0.1"}
	}
	raw, _ := json.Marshal(a)
	if len(raw) > maxBeaconBytes && len(a.IPs) > 1 {
		a.IPs = a.IPs[:1] // trim like the Python broadcaster
		raw, _ = json.Marshal(a)
	}
	return raw
}

func (b *Beacon) rateLimitedWarn(format string, args ...any) {
	now := time.Now()
	if now.Sub(b.lastWarn) >= warnRateLimitSecs*time.Second {
		b.lastWarn = now
		log.Printf("[discovery] "+format, args...)
	}
}

// newBroadcastConn opens a UDP4 socket with SO_BROADCAST + SO_REUSEADDR, bound
// to srcIP:0 ("0.0.0.0" = wildcard). Binding to a concrete source IP pins egress
// to that interface. Mirrors the Python broadcaster's socket options.
func newBroadcastConn(ctx context.Context, srcIP string) (net.PacketConn, error) {
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			var serr error
			if err := c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
			}); err != nil {
				return err
			}
			return serr
		},
	}
	return lc.ListenPacket(ctx, "udp4", net.JoinHostPort(srcIP, "0"))
}

// lanIface pairs a source IPv4 with its directed broadcast address and whether
// the owning interface supports broadcast (point-to-point links like Tailscale
// utun don't).
type lanIface struct {
	ip        string
	bcast     string
	broadcast bool
}

// lanIPv4Interfaces returns up interfaces' non-loopback IPv4s with their
// directed broadcast computed from the *actual* netmask (ip | ^mask). This is
// stricter than discovery_broadcaster.py's class-C a.b.c.255 guess, which is
// wrong on non-/24 LANs — e.g. a /22 (192.168.68.x, mask 255.255.252.0) has
// broadcast 192.168.71.255, and sending to .68.255 fails "host is down".
func lanIPv4Interfaces() []lanIface {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []lanIface
	seen := map[string]struct{}{}
	for i := range ifaces {
		ifi := &ifaces[i]
		if ifi.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, _ := ifi.Addrs()
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok || ipnet.IP.IsLoopback() {
				continue
			}
			ip4 := ipnet.IP.To4()
			if ip4 == nil {
				continue
			}
			s := ip4.String()
			if _, dup := seen[s]; dup {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, lanIface{
				ip:        s,
				bcast:     directedBroadcast(ip4, ipnet.Mask),
				broadcast: ifi.Flags&net.FlagBroadcast != 0,
			})
		}
	}
	return out
}

// directedBroadcast computes ip | ^mask. Falls back to the class-C a.b.c.255 if
// the mask is missing/non-IPv4.
func directedBroadcast(ip4 net.IP, mask net.IPMask) string {
	if len(mask) == net.IPv6len {
		mask = mask[12:] // IPv4-in-IPv6 mask → 4-byte tail
	}
	if len(mask) != net.IPv4len {
		return net.IPv4(ip4[0], ip4[1], ip4[2], 255).String()
	}
	bc := make(net.IP, net.IPv4len)
	for i := 0; i < net.IPv4len; i++ {
		bc[i] = ip4[i] | ^mask[i]
	}
	return bc.String()
}

// localIPv4s returns non-loopback IPv4 addresses (for the payload's ips list).
func localIPv4s() []string {
	var ips []string
	for _, ni := range lanIPv4Interfaces() {
		ips = append(ips, ni.ip)
	}
	return ips
}
