package core

import (
	"everything-go/internal/deviceinventory"
	"everything-go/internal/protocol"
	"testing"
)

func TestDeviceInventoryOnlyBoundPrimaryConnections(t *testing.T) {
	h, _ := newTestHub(t)
	var err error
	h.deviceInventory, err = deviceinventory.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = h.pairing.Claim("paired-token", "registered-device"); err != nil {
		t.Fatal(err)
	}
	h.syncDeviceInventory()
	c := newTestClient(h)
	info := &protocol.ClientInfo{Platform: "ios", AppID: "com.morrie.text", Version: "1.2.60", Build: "65", Channel: "apple-sandbox"}
	h.bindDeviceInventory(c, "wrong-token", "registered-device", "Phone", "ios", false)
	h.observeClientInfo(c, "registered-device", info)
	if h.deviceInventory.Snapshot().Devices[0].Info != nil {
		t.Fatal("unauthenticated report stored")
	}
	h.bindDeviceInventory(c, "paired-token", "spoof-device", "Phone", "ios", false)
	h.observeClientInfo(c, "spoof-device", info)
	if h.deviceInventory.Snapshot().Devices[0].Info != nil {
		t.Fatal("mismatched device report stored")
	}
	h.bindDeviceInventory(c, "paired-token", "registered-device", "Phone", "ios", true)
	h.observeClientInfo(c, "registered-device", info)
	h.touchDeviceInventory(c)
	if got := h.deviceInventory.Snapshot().Devices[0]; got.Info != nil || got.Online {
		t.Fatal("probe counted as active", got)
	}
	route(h, c, `{"type":"hello","device_id":"registered-device","client_surface":"ios"}`)
	route(h, c, `{"type":"ping","client_info":{"platform":"ios","app_id":"com.morrie.text","version":"1.2.60","build":"65","channel":"apple-sandbox"}}`)
	if got := h.deviceInventory.Snapshot().Devices[0]; got.Info == nil || got.Info.Build != "65" || !got.Online {
		t.Fatal(got)
	}
}
