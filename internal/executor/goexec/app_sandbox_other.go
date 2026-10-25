//go:build !darwin || !cgo

package goexec

import "errors"

func verifyInheritedAppSandbox() error { return errors.New("app_sandbox_macos_cgo_required") }
