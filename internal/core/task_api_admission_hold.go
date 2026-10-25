//go:build taskapihold

package core

// Compatible rollback: preserve scope enforcement, native evidence and ongoing
// effects while holding only new API create/append admission. Not a DB rewind.
const apiOwnedAdmissionEnabled = false
