package workitems

import "context"

// Service is the boundary consumed by core.Hub. WebSocket routing depends on
// this API, never on SQLite details, so storage migrations and synchronization
// policy remain local to this package.
type Service struct {
	store *Store
}

func OpenService(dataDir, instanceID string) (*Service, error) {
	store, err := Open(dataDir, instanceID)
	if err != nil {
		return nil, err
	}
	return &Service{store: store}, nil
}

func (s *Service) Close() error { return s.store.Close() }

func (s *Service) CreateProject(ctx context.Context, in CreateProjectInput) (Project, error) {
	return s.store.CreateProject(ctx, in)
}

func (s *Service) CreateItem(ctx context.Context, in CreateItemInput) (WorkItem, error) {
	return s.store.CreateItem(ctx, in)
}

func (s *Service) CreateWorkflow(ctx context.Context, in CreateWorkflowInput) (Workflow, error) {
	return s.store.CreateWorkflow(ctx, in)
}

func (s *Service) ImportSessionDraft(ctx context.Context, in ImportSessionDraftInput) (ImportSessionDraftResult, error) {
	return s.store.ImportSessionDraft(ctx, in)
}

func (s *Service) UpdateItem(ctx context.Context, in UpdateItemInput) (WorkItem, error) {
	return s.store.UpdateItem(ctx, in)
}

func (s *Service) MoveItem(ctx context.Context, in MoveItemInput) (WorkItem, error) {
	return s.store.MoveItem(ctx, in)
}

func (s *Service) DecideReview(ctx context.Context, in ReviewDecisionInput) (ReviewDecisionResult, error) {
	return s.store.DecideReview(ctx, in)
}

func (s *Service) LinkSession(ctx context.Context, in LinkSessionInput) (SessionLink, WorkItem, error) {
	return s.store.LinkSession(ctx, in)
}

func (s *Service) UnlinkSession(ctx context.Context, in UnlinkSessionInput) (SessionLink, WorkItem, error) {
	return s.store.UnlinkSession(ctx, in)
}

func (s *Service) AddDependency(ctx context.Context, in AddDependencyInput) (WorkItem, error) {
	return s.store.AddDependency(ctx, in)
}

func (s *Service) RemoveDependency(ctx context.Context, in AddDependencyInput) (WorkItem, error) {
	return s.store.RemoveDependency(ctx, in)
}

func (s *Service) ArchiveItem(ctx context.Context, in ArchiveItemInput) (WorkItem, error) {
	return s.store.ArchiveItem(ctx, in)
}

func (s *Service) AddComment(ctx context.Context, in AddCommentInput) (Comment, WorkItem, error) {
	return s.store.AddComment(ctx, in)
}

func (s *Service) EditComment(ctx context.Context, in EditCommentInput) (Comment, WorkItem, error) {
	return s.store.EditComment(ctx, in)
}

func (s *Service) AddAttachment(ctx context.Context, in AddAttachmentInput) (AttachmentRef, WorkItem, error) {
	return s.store.AddAttachment(ctx, in)
}

func (s *Service) RemoveAttachment(ctx context.Context, in RemoveAttachmentInput) (AttachmentRef, WorkItem, error) {
	return s.store.RemoveAttachment(ctx, in)
}

func (s *Service) GetItem(ctx context.Context, id string) (WorkItem, error) {
	return s.store.GetItem(ctx, id)
}

func (s *Service) GetProject(ctx context.Context, id string) (Project, error) {
	return s.store.GetProject(ctx, id)
}

func (s *Service) SaveBootstrapDraft(ctx context.Context, in SaveBootstrapInput) (SaveBootstrapResult, error) {
	return s.store.SaveBootstrapDraft(ctx, in)
}

func (s *Service) ApproveBootstrap(ctx context.Context, in ApproveBootstrapInput) (ApproveBootstrapResult, error) {
	return s.store.ApproveBootstrap(ctx, in)
}

func (s *Service) GetBootstrap(ctx context.Context, id string) (ProjectBootstrapDraft, error) {
	return s.store.GetBootstrap(ctx, id)
}

func (s *Service) BuildContextPack(ctx context.Context, itemID string, maxCharacters int) (ContextPack, error) {
	return s.store.BuildContextPack(ctx, itemID, maxCharacters)
}

func (s *Service) HasActiveRun(ctx context.Context, workItemID string) (bool, error) {
	return s.store.HasActiveRun(ctx, workItemID)
}

func (s *Service) Snapshot(ctx context.Context) (Snapshot, error) {
	return s.store.Snapshot(ctx)
}

func (s *Service) SnapshotForDevice(ctx context.Context, deviceID string) (DeviceSnapshot, error) {
	return s.store.SnapshotForDevice(ctx, deviceID)
}

func (s *Service) ChangesSince(ctx context.Context, since uint64, limit int) ([]Change, uint64, bool, error) {
	return s.store.ChangesSince(ctx, since, limit)
}

func (s *Service) AckSync(ctx context.Context, deviceID string, delivered, acked uint64) error {
	return s.store.AckSync(ctx, deviceID, delivered, acked)
}

func (s *Service) MarkRead(ctx context.Context, deviceID, itemID string, revision uint64) (ItemView, error) {
	return s.store.MarkRead(ctx, deviceID, itemID, revision)
}

func (s *Service) MutationResponse(ctx context.Context, deviceID, mutationID string) ([]byte, bool, error) {
	return s.store.MutationResponse(ctx, deviceID, mutationID)
}

func (s *Service) RememberMutation(ctx context.Context, deviceID, mutationID string, response []byte) error {
	return s.store.RememberMutation(ctx, deviceID, mutationID, response)
}

func (s *Service) CompactChanges(ctx context.Context) (int64, error) {
	return s.store.CompactChanges(ctx)
}

func (s *Service) StartRun(ctx context.Context, in StartRunInput) (Run, WorkItem, error) {
	return s.store.StartRun(ctx, in)
}

func (s *Service) RecoverQueue(ctx context.Context) (int64, error) { return s.store.RecoverQueue(ctx) }
func (s *Service) MarkRunSubmitted(ctx context.Context, runID string) error {
	return s.store.MarkRunSubmitted(ctx, runID)
}
func (s *Service) ClaimNextRun(ctx context.Context, now int64) (Run, WorkItem, bool, error) {
	return s.store.ClaimNextRun(ctx, now)
}
func (s *Service) DeferRun(ctx context.Context, runID string, availableAt int64, reason string) (Run, WorkItem, error) {
	return s.store.DeferRun(ctx, runID, availableAt, reason)
}
func (s *Service) EnqueueAutomaticRuns(ctx context.Context) ([]Run, error) {
	return s.store.EnqueueAutomaticRuns(ctx)
}

func (s *Service) OwnsRequest(ctx context.Context, sessionID, requestID string) (bool, error) {
	return s.store.OwnsRequest(ctx, sessionID, requestID)
}

func (s *Service) AdvanceRun(ctx context.Context, sessionID, requestID, status, reason string) (RunUpdate, error) {
	return s.store.AdvanceRun(ctx, sessionID, requestID, status, reason)
}

func (s *Service) Diagnostics(ctx context.Context) (Diagnostics, error) {
	return s.store.Diagnostics(ctx)
}
