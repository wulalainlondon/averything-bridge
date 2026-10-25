package main

import (
	"context"
	"everything-go/internal/core"
	"everything-go/internal/filetransfer"
	"everything-go/internal/linebridge"
	"os"
	"path/filepath"
	"strings"
)

func configuredLineService(hub *core.Hub, dataDir, authority string, transfers *filetransfer.Service) (*linebridge.Service, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	runtimeConfig, err := readLineConfig(dataDir, executable)
	if err != nil || runtimeConfig == nil {
		return nil, err
	}
	helper := runtimeConfig.Helper
	dir, err := filepath.Abs(filepath.Join(dataDir, "line"))
	if err != nil {
		return nil, err
	}
	key, err := linebridge.StoreKey(context.Background(), helper, dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		for i := range key {
			key[i] = 0
		}
	}()
	store, err := linebridge.OpenStore(dir, authority, key)
	if err != nil {
		return nil, err
	}
	config := linebridge.Config{VoiceDispatchAccounts: runtimeConfig.VoiceDispatchAccounts, VoiceDispatchSubjectAllowed: hub.LineVoiceDispatchSubjectAllowed, BlockedScopes: runtimeConfig.Blocked, Subjects: hub.LineSubjects, DataDir: dir, KeychainHelper: helper, LegacyDir: runtimeConfig.Legacy, EventSink: hub.LineEventSink(store), Resources: func(ctx context.Context, c linebridge.Caller, ref string) (linebridge.ResourceInput, error) {
		if strings.HasPrefix(c.Subject, "device:") {
			data, name, mime, err := transfers.ReadCompletedForDevice(strings.TrimPrefix(c.Subject, "device:"), ref, linebridge.MaxResourceBytes)
			if err != nil {
				return linebridge.ResourceInput{}, linebridge.Fail("line_resource_source_invalid")
			}
			return linebridge.ResourceInput{Bytes: data, Name: name, MIME: mime}, nil
		}
		return hub.ResolveLineAttachment(ctx, c, ref)
	}, SubjectAllowed: hub.LineSubjectAllowed}
	service := linebridge.NewService(store, linebridge.ExecFactory{Node: runtimeConfig.Node, Entry: runtimeConfig.Entry, Shared: runtimeConfig.Shared, Manifest: runtimeConfig.Manifest}, config)
	hub.SetLineService(service)
	if !runtimeConfig.Blocked["call"] {
		hub.SetLineVoiceRuntime(runtimeConfig.Node, filepath.Join(runtimeConfig.Shared, "route-worker.mjs"))
	}
	if runtimeConfig.RestoreExisting {
		if e := service.StartRestoreReport(filepath.Join(dataDir, "line-service-health.json")); e != nil {
			service.Close()
			store.Close()
			return nil, e
		}
	}
	service.StartScheduler()
	return service, nil
}
