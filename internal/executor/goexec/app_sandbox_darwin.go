//go:build darwin && cgo

package goexec

/*
#cgo LDFLAGS: -framework Security -framework CoreFoundation
#include <Security/Security.h>
#include <CoreFoundation/CoreFoundation.h>
static int inheritedAppSandbox(void) {
 SecCodeRef code=NULL; CFDictionaryRef info=NULL;
 if(SecCodeCopySelf(kSecCSDefaultFlags,&code)!=errSecSuccess) return 0;
 OSStatus status=SecCodeCopySigningInformation(code,kSecCSSigningInformation,&info);
 CFRelease(code); if(status!=errSecSuccess || !info) return 0;
 CFDictionaryRef ent=(CFDictionaryRef)CFDictionaryGetValue(info,kSecCodeInfoEntitlementsDict);
 int ok=ent && CFDictionaryGetValue(ent,CFSTR("com.apple.security.app-sandbox"))==kCFBooleanTrue
  && CFDictionaryGetValue(ent,CFSTR("com.apple.security.inherit"))==kCFBooleanTrue
  && CFDictionaryGetValue(ent,CFSTR("com.apple.security.get-task-allow"))!=kCFBooleanTrue;
 CFRelease(info); return ok;
}
*/
import "C"
import "errors"

func verifyInheritedAppSandbox() error {
	if C.inheritedAppSandbox() != 1 {
		return errors.New("app_sandbox_inherited_signed_process_required")
	}
	return nil
}
