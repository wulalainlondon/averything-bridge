package core

// reserveOfflineDevice atomically gives an authenticated QA connection an
// unused device slot. It never replaces a live user connection. A normal
// client may still reconnect later using the existing latest-device policy;
// the guarded client must not reclaim the slot and evict that user.
func (h *Hub) reserveOfflineDevice(c *Client) bool {
	h.latestMu.Lock()
	defer h.latestMu.Unlock()
	if previous := h.latestByDevice[c.deviceID]; previous != nil && previous != c {
		// live() takes latestMu too. The slot is already the current client;
		// only its quit signal is needed while we hold this lock.
		select {
		case <-previous.quit:
		default:
			return false
		}
	}
	h.latestByDevice[c.deviceID] = c
	return true
}
