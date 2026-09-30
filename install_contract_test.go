package install_test

import (
	"os"
	"strings"
	"testing"
)

func TestLaunchWrapperLoadsMonitoringEnvironmentBeforeExec(t *testing.T) {
	script, err := os.ReadFile("install.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(script)
	hook := `if [ -r "$RUNTIME_DIR/monitoring_env.sh" ]; then`
	source := `. "$RUNTIME_DIR/monitoring_env.sh"`
	execBridge := `exec "$SERVICE_BIN"`
	hookIndex := strings.Index(text, hook)
	sourceIndex := strings.Index(text, source)
	execIndex := strings.Index(text, execBridge)
	if hookIndex < 0 || sourceIndex < 0 {
		t.Fatal("generated launch wrapper must load monitoring_env.sh when readable")
	}
	if execIndex < 0 || hookIndex > execIndex || sourceIndex > execIndex {
		t.Fatal("monitoring environment must be loaded before the bridge process is executed")
	}
}
