package core

import (
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"unicode"
	"unicode/utf8"
)

func (h *Hub) mediaEndpoint(c *Client) *backend.MediaEndpoint {
	e := &backend.MediaEndpoint{BridgeInstanceID: h.cfg.InstanceID, ClientID: c.clientID, Platform: c.clientSurface, Verification: "bound_current_call"}
	if call := h.voiceClients[c]; call != nil {
		e.VoiceID = call.voiceID
		e.Model = call.mediaDeviceModel
	}
	if e.Platform == "" {
		e.Platform = "unknown"
	}
	if b := c.inventoryBinding.Load(); b != nil {
		e.DeviceInventoryID = b.key
		e.Name = b.name
		if h.deviceInventory != nil {
			for _, d := range h.deviceInventory.Snapshot().Devices {
				if d.ID == b.key {
					if d.Info != nil {
						e.Platform = d.Info.Platform
						e.AppVersion = d.Info.Version
						e.AppBuild = d.Info.Build
					}
					break
				}
			}
		}
	}
	return e
}
func safeMediaLabel(s string) bool {
	if !utf8.ValidString(s) || len([]rune(s)) > 80 {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
func validMediaAudio(a *protocol.MediaAudioInfo) bool {
	if !safeMediaLabel(a.DeviceModel) || !safeMediaLabel(a.CommunicationDevice) || !safeMediaLabel(a.SharedOutputDevice) || len(a.PredictedMediaRoutes) > 8 {
		return false
	}
	switch a.AudioProfile {
	case "", "standard", "car_media", "bluetooth_media", "phone_media":
	default:
		return false
	}
	for _, observation := range []string{a.SpeechOutputObservation, a.MicrophoneObservation} {
		switch observation {
		case "", "unknown", "actual_track":
		default:
			return false
		}
	}
	switch a.SpeechOutputRoute {
	case "", "speaker", "earpiece", "wired", "bluetooth_media", "bluetooth_hfp", "bluetooth_le", "usb", "other":
	default:
		return false
	}
	switch a.MicrophoneInput {
	case "", "phone", "other":
	default:
		return false
	}
	expectedRoute := "bluetooth_media"
	if a.AudioProfile == "phone_media" {
		expectedRoute = "speaker"
	}
	if a.SharedRouteVerified != nil && *a.SharedRouteVerified &&
		((a.AudioProfile != "bluetooth_media" && a.AudioProfile != "car_media" && a.AudioProfile != "phone_media") || a.SpeechOutputObservation != "actual_track" || a.SpeechOutputRoute != expectedRoute || a.MicrophoneObservation != "actual_track" || a.MicrophoneInput != "phone") {
		return false
	}
	switch a.CommunicationProfile {
	case "", "unknown", "bluetooth_hfp", "bluetooth_le", "wired", "speaker", "earpiece":
	default:
		return false
	}
	switch a.MediaRoutingObservation {
	case "", "unknown", "predicted":
	default:
		return false
	}
	for _, r := range a.PredictedMediaRoutes {
		switch r {
		case "speaker", "earpiece", "wired", "bluetooth_media", "bluetooth_hfp", "bluetooth_le", "usb", "other":
		default:
			return false
		}
	}
	return true
}
