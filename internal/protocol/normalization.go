package protocol

import (
	"net/url"
	"path"
	"regexp"
	"strings"
)

var windowsAbsolutePath = regexp.MustCompile(`^[A-Za-z]:/`)

func CanonicalEndpoint(value string) (string, bool) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil {
		return "", false
	}
	switch strings.ToLower(parsed.Scheme) {
	case "ws", "wss", "http", "https", "unix":
	default:
		return "", false
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	if (parsed.Scheme == "wss" || parsed.Scheme == "https") && strings.HasSuffix(parsed.Host, ":443") {
		parsed.Host = strings.TrimSuffix(parsed.Host, ":443")
	} else if (parsed.Scheme == "ws" || parsed.Scheme == "http") && strings.HasSuffix(parsed.Host, ":80") {
		parsed.Host = strings.TrimSuffix(parsed.Host, ":80")
	}
	parsed.User = nil
	parsed.Fragment = ""
	if parsed.Scheme == "unix" {
		return "unix://" + strings.ReplaceAll(url.PathEscape(parsed.Path), "%2F", "/"), true
	}
	if parsed.Path == "/" {
		parsed.Path = ""
	} else {
		parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	}
	return strings.TrimSuffix(parsed.String(), "/"), true
}

func CanonicalLocalPath(value string) (string, bool) {
	normalized := strings.ReplaceAll(strings.TrimSpace(value), `\`, "/")
	if !strings.HasPrefix(normalized, "/") && !windowsAbsolutePath.MatchString(normalized) {
		return "", false
	}
	return path.Clean(normalized), true
}

func LegacyEpochToUnixMilli(value int64) int64 {
	if value > -100_000_000_000 && value < 100_000_000_000 {
		return value * 1000
	}
	return value
}
