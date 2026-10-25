package install_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func installerShell(t *testing.T, code string, env ...string) (string, error) {
	t.Helper()
	script, err := filepath.Abs("install.sh")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "-c", `source "$INSTALLER"`+"\n"+code)
	// Tests never run main or a system service. The test-owned filesystem is isolated.
	cmd.Env = append(os.Environ(), append([]string{"INSTALLER=" + script}, env...)...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestInstallerRequiresExplicitReleaseAndLocalChecksum(t *testing.T) {
	for _, tc := range []struct {
		name, source, tag, checksum string
		ok                          bool
	}{
		{name: "no default"},
		{name: "local without checksum", source: "archive"},
		{name: "local explicit", source: "archive", checksum: strings.Repeat("a", 64), ok: true},
		{name: "ambiguous", source: "archive", tag: "v0.2.171", checksum: strings.Repeat("a", 64)},
		{name: "fixed tag", tag: "v0.2.171", ok: true},
		{name: "latest refused", tag: "latest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			archive := ""
			if tc.source != "" {
				archive = filepath.Join(dir, "archive.zip")
				if err := os.WriteFile(archive, []byte("fixture"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			out, err := installerShell(t, `LABEL=com.test; PORT=8766; TUNNEL_ENABLED=0; OS=darwin; RUNTIME_DIR="$TEST_DIR/runtime"; SESSION_STORE="$RUNTIME_DIR/saved_sessions.json"; REPO=wulalainlondon/everything-go; EXPECTED_SOURCE=''; validate_config`, "TEST_DIR="+dir, "LOCAL_ARCHIVE="+archive, "RELEASE_TAG="+tc.tag, "LOCAL_SHA="+tc.checksum)
			if (err == nil) != tc.ok {
				t.Fatalf("ok=%v err=%v output=%s", tc.ok, err, out)
			}
			if _, err := os.Stat(filepath.Join(dir, "runtime")); !os.IsNotExist(err) {
				t.Fatal("validation wrote runtime")
			}
		})
	}
}

func TestInstallerRefusesExistingRuntimeWithoutWriting(t *testing.T) {
	dir := t.TempDir()
	runtime := filepath.Join(dir, "runtime")
	if err := os.Mkdir(runtime, 0700); err != nil {
		t.Fatal(err)
	}
	pairing := filepath.Join(runtime, "pairing.json")
	original := []byte("private-fixture-identity")
	if err := os.WriteFile(pairing, original, 0600); err != nil {
		t.Fatal(err)
	}
	out, err := installerShell(t, `LAUNCH="$RUNTIME_DIR/everything_go_launch.sh"; APP_DIR="$RUNTIME_DIR/Everything Go.app"; BIN="$RUNTIME_DIR/everything-go"; SESSION_STORE="$RUNTIME_DIR/saved_sessions.json"; OS=darwin; assert_fresh_target`, "RUNTIME_DIR="+runtime)
	if err == nil || !strings.Contains(out, "runtime is not empty") {
		t.Fatalf("err=%v output=%s", err, out)
	}
	after, err := os.ReadFile(pairing)
	if err != nil || string(after) != string(original) {
		t.Fatal("original identity changed")
	}
	entries, _ := os.ReadDir(runtime)
	if len(entries) != 1 {
		t.Fatal("refusal changed target")
	}
}

func TestGeneratedWrapperExactArgumentsAndMonitoringBeforeExec(t *testing.T) {
	for _, tunnel := range []string{"0", "1"} {
		t.Run("tunnel"+tunnel, func(t *testing.T) {
			dir := t.TempDir()
			runtime := filepath.Join(dir, `space & quote' dollar$ runtime`)
			if err := os.Mkdir(runtime, 0700); err != nil {
				t.Fatal(err)
			}
			bin := filepath.Join(runtime, `fake bin`)
			capture := filepath.Join(dir, "captured.json")
			// A real child process captures argv and an environment value loaded from monitoring_env.sh.
			python, err := exec.LookPath("python3")
			if err != nil {
				t.Skip("python3 unavailable")
			}
			body := "#!" + python + "\nimport json,os,sys\nwith open(os.environ['CAPTURE'],'w') as f: json.dump({'args':sys.argv[1:],'monitor':os.environ.get('MONITOR_READY')},f)\n"
			if err := os.WriteFile(bin, []byte(body), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(runtime, "monitoring_env.sh"), []byte("export MONITOR_READY=before-exec\n"), 0600); err != nil {
				t.Fatal(err)
			}
			code := `LAUNCH="$RUNTIME_DIR/launch.sh"; SERVICE_PATH="$PATH"; PORT=8766; SESSION_STORE="$RUNTIME_DIR/saved sessions.json"; INSTANCE_NAME='computer $(must-not-run)'; CLAUDE_BIN=''; CODEX_BIN='/exact CLI/codex';
BRIDGE_BROWSER_ORIGIN_MODE=ask; BRIDGE_BROWSER_ALLOWED_ORIGINS=''; BRIDGE_BROWSER_MANAGE_AUTO_REVIEW=1; BRIDGE_BROWSER_ENABLE_NETWORK=1;
EVERYTHING_GO_CODEX_SESSIONS_DIR="$RUNTIME_DIR/native sessions"; EVERYTHING_GO_CODEX_IGNORE_CWD_GLOBS=''; EVERYTHING_GO_CODEX_IGNORE_NAME_PREFIXES=''; EVERYTHING_GO_CODEX_ROLLOVER_ENABLED=false; EVERYTHING_GO_CODEX_COLD_RESUME_MAX_BYTES=268435456; EVERYTHING_GO_CODEX_CHECKPOINT_MAX_BYTES=131072; EVERYTHING_GO_CODEX_APP_SERVER_MODE=daemon; EVERYTHING_GO_CODEX_APP_SERVER_SOCKET='';
write_launch_wrapper
bash "$LAUNCH"`
			out, err := installerShell(t, code, "RUNTIME_DIR="+runtime, "SERVICE_BIN="+bin, "CAPTURE="+capture, "TUNNEL_ENABLED="+tunnel)
			if err != nil {
				t.Fatalf("err=%v output=%s", err, out)
			}
			raw, err := os.ReadFile(capture)
			if err != nil {
				t.Fatal(err)
			}
			var got struct {
				Args    []string `json:"args"`
				Monitor string   `json:"monitor"`
			}
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got.Monitor != "before-exec" {
				t.Fatal("monitoring not loaded before execution")
			}
			want := []string{"--port", "8766", "--executor", "go", "--data-dir", runtime, "--session-store", filepath.Join(runtime, "saved sessions.json"), "--discovery", "--mdns", "--instance-name", "computer $(must-not-run)", "--codex-bin", "/exact CLI/codex"}
			if tunnel == "1" {
				want = append(want, "--tunnel")
			}
			if strings.Join(got.Args, "\x00") != strings.Join(want, "\x00") {
				t.Fatalf("argv mismatch: %q", got.Args)
			}
		})
	}
}

func TestLaunchPlistUsesDistinctArgumentsAndEscapesPaths(t *testing.T) {
	dir := t.TempDir()
	plist := filepath.Join(dir, "agent.plist")
	runtime := filepath.Join(dir, `space & <xml> "quote"`)
	out, err := installerShell(t, `HOME="$TEST_DIR"; LABEL=com.test; LAUNCH="$RUNTIME_DIR/launch.sh"; write_launchd_plist`, "TEST_DIR="+dir, "RUNTIME_DIR="+runtime, "PLIST="+plist)
	if err != nil {
		t.Fatalf("err=%v output=%s", err, out)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	cmd := exec.Command(python, "-c", `import plistlib,sys; p=plistlib.load(open(sys.argv[1],'rb')); assert p['ProgramArguments']==['/bin/bash',sys.argv[2]+'/launch.sh']; assert p['WorkingDirectory']==sys.argv[2]`, plist, runtime)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("plist invalid: %v %s", err, out)
	}
}

func TestInstallerChecksumFailureCleansStaging(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "archive.zip")
	if err := os.WriteFile(archive, []byte("fixture-invalid-checksum"), 0600); err != nil {
		t.Fatal(err)
	}
	out, err := installerShell(t, `OS=darwin; trap cleanup EXIT; prepare_package`, "TMPDIR="+dir, "LOCAL_ARCHIVE="+archive, "LOCAL_SHA="+strings.Repeat("0", 64))
	if err == nil || !strings.Contains(out, "archive checksum mismatch") {
		t.Fatalf("err=%v output=%s", err, out)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != "archive.zip" {
		t.Fatal("temporary staging was not cleaned")
	}
}

func TestFreshActivationUsesOneBootstrapAndChecksBinaryOwnership(t *testing.T) {
	dir := t.TempDir()
	code := `HOME="$TEST_DIR"; OS=darwin; LABEL=com.test; PORT=18766; RUNTIME_DIR="$TEST_DIR/runtime"; mkdir "$RUNTIME_DIR"; LAUNCH="$RUNTIME_DIR/launch.sh"; PLIST="$TEST_DIR/agent.plist"; SERVICE_BIN="$RUNTIME_DIR/exact bin";
launchctl() {
 case "$1" in
  bootstrap) printf 'bootstrap\n' >> "$TEST_DIR/calls" ;;
  print) printf '    pid = 123\n' ;;
  *) return 77 ;;
 esac
}
lsof() { case " $* " in *' -d txt '*) printf 'i999\nn%s\n' "$SERVICE_BIN" ;; *) printf '123\n' ;; esac; }
stat() { printf '999\n'; }
activate_fresh_service`
	out, err := installerShell(t, code, "TEST_DIR="+dir)
	if err != nil {
		t.Fatalf("err=%v output=%s", err, out)
	}
	calls, err := os.ReadFile(filepath.Join(dir, "calls"))
	if err != nil || string(calls) != "bootstrap\n" {
		t.Fatalf("unexpected bootstrap calls: %q %v", calls, err)
	}
	if !strings.Contains(out, "binary/listener verified") {
		t.Fatalf("missing health result: %s", out)
	}
}

func TestUncertainBootstrapDoesNotReplayOrRestart(t *testing.T) {
	dir := t.TempDir()
	code := `HOME="$TEST_DIR"; OS=darwin; LABEL=com.test; PORT=18766; RUNTIME_DIR="$TEST_DIR/runtime"; LAUNCH="$RUNTIME_DIR/launch.sh"; PLIST="$TEST_DIR/agent.plist";
launchctl() { printf '%s\n' "$1" >> "$TEST_DIR/calls"; return 7; }
activate_fresh_service`
	out, err := installerShell(t, code, "TEST_DIR="+dir)
	if err == nil || !strings.Contains(out, "failed or uncertain") {
		t.Fatalf("err=%v output=%s", err, out)
	}
	calls, readErr := os.ReadFile(filepath.Join(dir, "calls"))
	if readErr != nil || string(calls) != "bootstrap\n" {
		t.Fatalf("replayed or changed service operation: %q %v", calls, readErr)
	}
}
