package session

import (
	"errors"
	"sync"
)

// Configuration is persisted with each admitted message. It deliberately
// excludes cwd, resume handles and ownership so queued work cannot change them.
type Configuration struct {
	Revision          uint64 `json:"revision"`
	Backend           string `json:"backend"`
	Model             string `json:"model"`
	Sandbox           string `json:"sandbox"`
	Effort            string `json:"effort"`
	ServiceTier       string `json:"service_tier"`
	CollaborationMode string `json:"collaboration_mode"`
	Personality       string `json:"personality"`
}

func ConfigurationFrom(s Snapshot) Configuration {
	return Configuration{s.ConfigRevision, s.Backend, s.Model, s.Sandbox, s.Effort, s.ServiceTier, s.CollaborationMode, s.Personality}
}

func (c Configuration) applySnapshot(s Snapshot) Snapshot {
	s.ConfigRevision, s.Backend, s.Model, s.Sandbox, s.Effort = c.Revision, c.Backend, c.Model, c.Sandbox, c.Effort
	s.ServiceTier, s.CollaborationMode, s.Personality = c.ServiceTier, c.CollaborationMode, c.Personality
	return s
}

func (s *Session) applyConfigurationLocked(c Configuration) {
	s.configRevision, s.backend, s.model, s.sandbox, s.effort = c.Revision, c.Backend, c.Model, c.Sandbox, c.Effort
	s.serviceTier, s.collaborationMode, s.personality = c.ServiceTier, c.CollaborationMode, c.Personality
}

// SettingsSnapshot is the default for newly admitted work. Snapshot remains
// the active execution snapshot, including while future defaults are edited.
func (s *Session) SettingsSnapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := s.snapshotLocked()
	if s.futureConfig != nil {
		snap = s.futureConfig.applySnapshot(snap)
	}
	return snap
}

// The Hub holds messageQueueMu for update + durable commit + admission. This
// method additionally excludes config RPC/dequeue transitions under s.mu.
func (s *Session) SetFutureConfiguration(c Configuration, expected *uint64) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	before := s.snapshotLocked()
	if s.futureConfig != nil {
		before = s.futureConfig.applySnapshot(before)
	}
	if s.state == Closed || s.configUpdating {
		return before, errors.New("session_busy")
	}
	if expected != nil && *expected != before.ConfigRevision {
		return before, errors.New("config_revision_conflict")
	}
	if c.Backend != before.Backend || c.Sandbox != before.Sandbox {
		return before, errors.New("next_message_cannot_change_backend_or_permissions")
	}
	c.Revision = before.ConfigRevision + 1
	s.futureConfig = &c
	return c.applySnapshot(before), nil
}

func (s *Session) RestoreFutureConfiguration(before Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := ConfigurationFrom(before)
	s.futureConfig = &c
}

// Called only by the serial worker after durable queue admission. The default
// configuration remains unchanged even when an older queued revision runs.
func (s *Session) ActivateQueuedConfiguration(c Configuration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != Streaming || c.Backend != s.backend || c.Sandbox != s.sandbox {
		return errors.New("queued_configuration_no_longer_compatible")
	}
	if s.futureConfig == nil {
		current := ConfigurationFrom(s.snapshotLocked())
		s.futureConfig = &current
	}
	s.applyConfigurationLocked(c)
	return nil
}

// Configuration updates share the mailbox dequeue boundary. New turns cannot
// start halfway through an asynchronous runtime settings RPC.
func (s *Session) ReserveConfiguration(expected *uint64) (Snapshot, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	before := s.snapshotLocked()
	if s.futureConfig != nil {
		before = s.futureConfig.applySnapshot(before)
	}
	if s.state != Idle || len(s.mailbox) != 0 || s.queueHolds != 0 || s.configUpdating {
		return before, nil, errors.New("session_busy")
	}
	if expected != nil && *expected != before.ConfigRevision {
		return before, nil, errors.New("config_revision_conflict")
	}
	if s.futureConfig != nil {
		before = s.futureConfig.applySnapshot(before)
		if expected != nil && *expected != before.ConfigRevision {
			return before, nil, errors.New("config_revision_conflict")
		}
		s.applyConfigurationLocked(*s.futureConfig)
		s.futureConfig = nil
	}
	s.configUpdating = true
	s.queueHolds++
	var once sync.Once
	return before, func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.configUpdating = false
			if s.state != Closed {
				s.queueHolds--
			}
			s.signalQueueLocked()
		})
	}, nil
}

func (s *Session) SetConfigRevision(revision uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configRevision = revision
}
