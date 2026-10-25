package workitems

import "everything-go/internal/taskapi"

func (s *Store) TaskJournal() *taskapi.Journal { return taskapi.AttachedJournal(s.db, "pm") }

func (s *Service) TaskJournal() *taskapi.Journal { return s.store.TaskJournal() }
