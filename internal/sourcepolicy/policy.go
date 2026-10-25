// Package sourcepolicy centralizes provider session-source isolation.
package sourcepolicy

import (
	"os"
	"path/filepath"
	"strings"
)

func CodexSessionsDir() string {
	if value := expand(strings.TrimSpace(os.Getenv("EVERYTHING_GO_CODEX_SESSIONS_DIR"))); value != "" {
		return filepath.Clean(value)
	}
	if value := expand(strings.TrimSpace(os.Getenv("CODEX_HOME"))); value != "" {
		return filepath.Join(filepath.Clean(value), "sessions")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".codex", "sessions")
}

func CodexHome() string {
	return filepath.Dir(CodexSessionsDir())
}

func ClaudeConfigDir() string {
	if value := strings.TrimSpace(os.Getenv("CLAUDE_CONFIG_DIR")); value != "" {
		value = expand(value)
		if absolute, err := filepath.Abs(value); err == nil {
			return absolute
		}
		return filepath.Clean(value)
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".claude")
}

func ClaudeProjectsDir() string { return filepath.Join(ClaudeConfigDir(), "projects") }

func CodexIgnoreCWDGlobs() []string {
	return splitList(os.Getenv("EVERYTHING_GO_CODEX_IGNORE_CWD_GLOBS"))
}

func CodexIgnoreNamePrefixes() []string {
	return splitList(os.Getenv("EVERYTHING_GO_CODEX_IGNORE_NAME_PREFIXES"))
}

func IgnoreCodexSession(cwd, name string, cwdGlobs, namePrefixes []string) bool {
	for _, prefix := range namePrefixes {
		if strings.HasPrefix(strings.TrimSpace(name), prefix) {
			return true
		}
	}
	if strings.TrimSpace(cwd) == "" {
		return false
	}
	cleanCWD := filepath.Clean(expand(strings.TrimSpace(cwd)))
	for _, raw := range cwdGlobs {
		pattern := filepath.Clean(expand(strings.TrimSpace(raw)))
		if pattern == "" || pattern == "." {
			continue
		}
		if strings.HasSuffix(pattern, string(filepath.Separator)+"**") {
			root := strings.TrimSuffix(pattern, string(filepath.Separator)+"**")
			if cleanCWD == root || strings.HasPrefix(cleanCWD, root+string(filepath.Separator)) {
				return true
			}
		}
		if !strings.ContainsAny(pattern, "*?[") &&
			(cleanCWD == pattern || strings.HasPrefix(cleanCWD, pattern+string(filepath.Separator))) {
			return true
		}
		if matched, err := filepath.Match(pattern, cleanCWD); err == nil && matched {
			return true
		}
	}
	return false
}

func splitList(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n'
	})
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		if value := strings.TrimSpace(field); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func expand(value string) string {
	value = os.ExpandEnv(value)
	if value == "~" {
		home, _ := os.UserHomeDir()
		return home
	}
	if strings.HasPrefix(value, "~"+string(filepath.Separator)) {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, strings.TrimPrefix(value, "~"+string(filepath.Separator)))
	}
	return value
}
