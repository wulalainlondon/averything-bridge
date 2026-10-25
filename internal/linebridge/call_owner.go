package linebridge

import "context"

type callOwner struct {
	account string
	cancel  context.CancelFunc
	done    chan struct{}
}

func (s *Service) RegisterCall(ctx context.Context, c Caller, r CallReceipt, cancel context.CancelFunc) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return nil, Fail("line_service_closed")
	}
	if _, err := s.account(ctx, c, r.Account, "call"); err != nil {
		return nil, err
	}
	owner := &callOwner{account: r.Account, cancel: cancel, done: make(chan struct{})}
	s.calls[r.ID] = owner
	return func() {
		s.mu.Lock()
		if s.calls[r.ID] == owner {
			delete(s.calls, r.ID)
			close(owner.done)
		}
		s.mu.Unlock()
	}, nil
}
func (s *Service) stopAccountCalls(account string) {
	s.mu.Lock()
	owners := []*callOwner{}
	for _, owner := range s.calls {
		if owner.account == account {
			owners = append(owners, owner)
		}
	}
	s.mu.Unlock()
	for _, owner := range owners {
		owner.cancel()
	}
}
