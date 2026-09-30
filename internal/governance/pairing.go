// Package governance implements connection-governance: bridge pairing and the
// offline event buffer that lets reconnecting clients recover missed events.
package governance

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	ErrClaimedByAnother = errors.New("Bridge pairing is closed; open a pairing window from a trusted device")
	ErrTokenMismatch    = errors.New("Unauthorized: token mismatch")
)

const pairingSchemaVersion = 2
const initialEnrollmentDuration = 10 * time.Minute

type pairedDevice struct {
	Token    string `json:"token"`
	DeviceID string `json:"device_id"`
	PairedAt int64  `json:"paired_at"`
}

// DeviceBinding is an internal snapshot. Credentials are never JSON-encoded.
type DeviceBinding struct {
	Token    string `json:"-"`
	DeviceID string `json:"-"`
}

func (p *Pairing) DeviceBindings() []DeviceBinding {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]DeviceBinding, 0, len(p.devices))
	for _, device := range p.devices {
		out = append(out, DeviceBinding{Token: device.Token, DeviceID: device.DeviceID})
	}
	return out
}

type pairingFile struct {
	Version int            `json:"version,omitempty"`
	Devices []pairedDevice `json:"devices,omitempty"`

	// v1 migration fields. They are read but never emitted by v2.
	PairedToken    string `json:"paired_token,omitempty"`
	PairedDeviceID string `json:"paired_device_id,omitempty"`
	PairedAt       int64  `json:"paired_at,omitempty"`
}

// Pairing persists one credential per trusted device. The first device may
// claim an unpaired bridge. Additional devices require a short-lived pairing
// window opened by an already authenticated client.
type Pairing struct {
	mu   sync.Mutex
	path string

	devices     map[string]pairedDevice // token -> device
	enrollUntil time.Time
}

func NewPairing(path string) *Pairing {
	p := &Pairing{path: path, devices: make(map[string]pairedDevice)}
	if data, err := os.ReadFile(path); err == nil {
		var f pairingFile
		if json.Unmarshal(data, &f) == nil {
			for _, device := range f.Devices {
				if device.Token != "" {
					p.devices[device.Token] = device
				}
			}
			if len(p.devices) == 0 && f.PairedToken != "" {
				p.devices[f.PairedToken] = pairedDevice{
					Token: f.PairedToken, DeviceID: f.PairedDeviceID, PairedAt: f.PairedAt,
				}
			}
		}
	}
	if len(p.devices) == 0 {
		p.enrollUntil = time.Now().Add(initialEnrollmentDuration)
	}
	return p
}

func (p *Pairing) IsLocked() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.devices) > 0
}

func (p *Pairing) LockedTo(token string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.devices[token]
	return token != "" && ok
}

func (p *Pairing) OpenEnrollment(duration time.Duration) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	if duration <= 0 {
		duration = 2 * time.Minute
	}
	p.enrollUntil = time.Now().Add(duration)
	return p.enrollUntil
}

func (p *Pairing) EnrollmentOpen() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return time.Now().Before(p.enrollUntil)
}

func (p *Pairing) closeEnrollmentLocked() { p.enrollUntil = time.Time{} }

// Claim registers token for deviceID. The first device can always claim; a new
// token on an existing bridge is accepted only while enrollment is open.
func (p *Pairing) Claim(token, deviceID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if token == "" {
		return ErrTokenMismatch
	}
	if existing, ok := p.devices[token]; ok {
		existing.DeviceID = deviceID
		p.devices[token] = existing
		return p.saveLocked()
	}
	if len(p.devices) > 0 && !time.Now().Before(p.enrollUntil) {
		return ErrClaimedByAnother
	}
	p.devices[token] = pairedDevice{Token: token, DeviceID: deviceID, PairedAt: time.Now().Unix()}
	p.closeEnrollmentLocked() // one newly trusted device per explicit window
	return p.saveLocked()
}

// Unclaim revokes only the presented device credential. Other trusted devices
// remain connected and retain access.
func (p *Pairing) Unclaim(token string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.devices[token]; !ok {
		return ErrTokenMismatch
	}
	delete(p.devices, token)
	if len(p.devices) == 0 {
		p.enrollUntil = time.Now().Add(initialEnrollmentDuration)
		if p.path != "" {
			_ = os.Remove(p.path)
		}
		return nil
	}
	return p.saveLocked()
}

func (p *Pairing) saveLocked() error {
	if p.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p.path), 0o755); err != nil {
		return err
	}
	devices := make([]pairedDevice, 0, len(p.devices))
	for _, device := range p.devices {
		devices = append(devices, device)
	}
	sort.Slice(devices, func(i, j int) bool { return devices[i].Token < devices[j].Token })
	data, err := json.Marshal(pairingFile{Version: pairingSchemaVersion, Devices: devices})
	if err != nil {
		return err
	}
	tmp := p.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, p.path)
}
