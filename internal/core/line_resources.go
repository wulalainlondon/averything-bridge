package core

import (
	"context"
	"everything-go/internal/linebridge"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ResolveLineAttachment consumes the existing canonical attachment registry,
// scoped to the invoking conversation. Model-supplied filesystem paths are not
// accepted. Bytes are frozen and encrypted by LINE before a send is queued.
func (h *Hub) ResolveLineAttachment(ctx context.Context, c linebridge.Caller, ref string) (linebridge.ResourceInput, error) {
	if ctx.Err() != nil {
		return linebridge.ResourceInput{}, ctx.Err()
	}
	if h.attachments == nil || !strings.HasPrefix(c.Subject, "session:") {
		return linebridge.ResourceInput{}, linebridge.Fail("line_resource_source_invalid")
	}
	record, ok := h.attachments.Get(ref)
	sid := strings.TrimPrefix(c.Subject, "session:")
	if !ok || record.SessionID != sid || !h.LineSessionAllowed(sid) {
		return linebridge.ResourceInput{}, linebridge.Fail("line_resource_source_invalid")
	}
	root, err := os.OpenRoot(filepath.Dir(record.Path))
	if err != nil {
		return linebridge.ResourceInput{}, linebridge.Fail("line_resource_source_invalid")
	}
	defer root.Close()
	f, err := root.Open(filepath.Base(record.Path))
	if err != nil {
		return linebridge.ResourceInput{}, linebridge.Fail("line_resource_source_invalid")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > linebridge.MaxResourceBytes {
		return linebridge.ResourceInput{}, linebridge.Fail("line_resource_size_invalid")
	}
	data, err := io.ReadAll(io.LimitReader(f, linebridge.MaxResourceBytes+1))
	if err != nil || len(data) > linebridge.MaxResourceBytes {
		return linebridge.ResourceInput{}, linebridge.Fail("line_resource_size_invalid")
	}
	name := record.DisplayName
	if name == "" {
		name = filepath.Base(record.Path)
	}
	return linebridge.ResourceInput{Bytes: data, Name: name, MIME: record.MIMEType}, nil
}
