// codex-tool-probe performs only initialize, thread/read and MCP inventory reads.
package main

import (
	"context"
	"encoding/json"
	"everything-go/internal/executor/goexec"
	"everything-go/internal/toolenv"
	"flag"
	"os"
	"time"
)

func main() {
	socket := flag.String("socket", "", "existing daemon Unix socket (required)")
	thread := flag.String("thread", "", "existing thread ID (required)")
	flag.Parse()
	if *socket == "" || *thread == "" {
		flag.Usage()
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	snap, err := goexec.ProbeToolEnvironment(ctx, *socket, *thread)
	if err != nil {
		json.NewEncoder(os.Stdout).Encode(map[string]string{"error_code": toolenv.Code(err)})
		os.Exit(1)
	}
	json.NewEncoder(os.Stdout).Encode(snap)
}
