package main

import "testing"

func TestIsTailscaleIPv4(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"100.64.0.1", true},
		{"100.77.166.27", true},
		{"100.127.255.254", true},
		{"100.63.255.255", false},
		{"100.128.0.1", false},
		{"192.168.1.5", false},
		{"100.77.166.27:8777", false},
		{"", false},
	} {
		if got := isTailscaleIPv4(tc.value); got != tc.want {
			t.Errorf("isTailscaleIPv4(%q) = %v, want %v", tc.value, got, tc.want)
		}
	}
}
