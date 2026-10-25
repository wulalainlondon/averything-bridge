package core

import "testing"

func TestHelloResolvesCurrentLANAddressAfterNetworkChange(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.LanIP = "192.168.68.53"
	current := "192.168.68.58"
	h.cfg.ResolveLANIP = func() string { return current }
	for _, want := range []string{"192.168.68.58", "10.1.2.3", ""} {
		current = want
		c := newDeviceClient(h, "lan-probe", 8)
		c.inventoryProbe = true
		route(h, c, `{"type":"hello","device_id":"lan-probe","connection_probe":true}`)
		ack := waitForType(t, c, "hello_ack")
		got, _ := ack["lan_ip"].(string)
		if got != want {
			t.Fatalf("hello lan_ip=%q, want current address %q", got, want)
		}
	}
}
