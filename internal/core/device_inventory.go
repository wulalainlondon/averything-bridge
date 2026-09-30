package core

import (
	"context"
	"log"
	"strings"

	"everything-go/internal/deviceinventory"
	"everything-go/internal/protocol"
)

type clientInventoryBinding struct {
	key, deviceID, name, surface string
	probe                        bool
}

func (h *Hub) syncDeviceInventory() {
	if h.deviceInventory == nil {
		return
	}
	var bindings []deviceinventory.Binding
	for _, binding := range h.pairing.DeviceBindings() {
		bindings = append(bindings, deviceinventory.Binding{Token: binding.Token, DeviceID: binding.DeviceID})
	}
	if err := h.deviceInventory.SyncBindings(bindings); err != nil {
		log.Printf("[device-inventory] pairing sync failed: %v", err)
	}
}

func (h *Hub) bindDeviceInventory(c *Client, token, deviceID, name, surface string, probe bool) {
	if h.deviceInventory == nil || c.enrollmentOnly {
		return
	}
	key := h.deviceInventory.Bind(token, deviceID)
	if key == "" {
		return
	}
	c.inventoryBinding.Store(&clientInventoryBinding{key: key, deviceID: deviceID, name: name, surface: strings.ToLower(strings.TrimSpace(surface)), probe: probe})
}

func (h *Hub) observeClientInfo(c *Client, deviceID string, info *protocol.ClientInfo) {
	b := c.inventoryBinding.Load()
	if b == nil || b.probe || h.deviceInventory == nil || b.deviceID != deviceID {
		return
	}
	if err := h.deviceInventory.Observe(b.key, c.clientID, b.deviceID, b.name, b.surface, info); err != nil {
		log.Printf("[device-inventory] observation not saved: %v", err)
	}
}

func (h *Hub) touchDeviceInventory(c *Client) {
	if b := c.inventoryBinding.Load(); b != nil {
		h.observeClientInfo(c, b.deviceID, nil)
	}
}

func (h *Hub) StartDeviceInventoryAdmin(ctx context.Context) {
	if h.deviceInventory == nil {
		return
	}
	if _, err := deviceinventory.ServeAdmin(ctx, h.cfg.DataDir, h.deviceInventory.Snapshot); err != nil {
		log.Printf("[device-inventory] local management unavailable: %v", err)
	}
}
