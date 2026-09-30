package identity

import (
	"errors"
	"net/url"
	"strings"
)

const SessionKeyVersion = 1
const sessionKeyPrefix = "sk1:"

type SessionIdentity struct {
	AuthorityInstanceID string
	SessionID           string
}

func MakeSessionKey(authorityInstanceID, sessionID string) (string, error) {
	authorityInstanceID = strings.TrimSpace(authorityInstanceID)
	sessionID = strings.TrimSpace(sessionID)
	if authorityInstanceID == "" || sessionID == "" {
		return "", errors.New("session authority and local id are required")
	}
	return sessionKeyPrefix + escapeComponent(authorityInstanceID) + ":" + escapeComponent(sessionID), nil
}

func escapeComponent(value string) string {
	escaped := strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
	return strings.ReplaceAll(escaped, "%7E", "~")
}

func ParseSessionKey(value string) (SessionIdentity, bool) {
	if !strings.HasPrefix(value, sessionKeyPrefix) {
		return SessionIdentity{}, false
	}
	encoded := strings.TrimPrefix(value, sessionKeyPrefix)
	separator := strings.IndexByte(encoded, ':')
	if separator <= 0 || separator == len(encoded)-1 {
		return SessionIdentity{}, false
	}
	authority, err := url.QueryUnescape(encoded[:separator])
	if err != nil {
		return SessionIdentity{}, false
	}
	sessionID, err := url.QueryUnescape(encoded[separator+1:])
	if err != nil {
		return SessionIdentity{}, false
	}
	authority, sessionID = strings.TrimSpace(authority), strings.TrimSpace(sessionID)
	if authority == "" || sessionID == "" {
		return SessionIdentity{}, false
	}
	return SessionIdentity{AuthorityInstanceID: authority, SessionID: sessionID}, true
}

func WireSessionID(value string) string {
	if identity, ok := ParseSessionKey(value); ok {
		return identity.SessionID
	}
	return value
}
