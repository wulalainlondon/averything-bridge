import Foundation
import Security
import LocalAuthentication

// Credentials travel only through the private stdout pipe, never argv or files.
let arguments = CommandLine.arguments
guard arguments.count == 3, ["get", "create"].contains(arguments[1]),
      arguments[2].hasPrefix("tw.wulala.linejs-lab.") else { exit(64) }
let operation = arguments[1]
let service = arguments[2]
let query: [String: Any] = [
    kSecClass as String: kSecClassGenericPassword,
    kSecAttrService as String: service,
    kSecAttrAccount as String: "session-master-v1",
    kSecAttrSynchronizable as String: false
]
func readKey() -> (OSStatus, Data?) {
    var lookup = query
    lookup[kSecReturnData as String] = true
    lookup[kSecMatchLimit as String] = kSecMatchLimitOne
    let context = LAContext()
    context.interactionNotAllowed = true
    lookup[kSecUseAuthenticationContext as String] = context
    var item: CFTypeRef?
    let status = SecItemCopyMatching(lookup as CFDictionary, &item)
    return (status, item as? Data)
}
var (status, key) = readKey()
if status == errSecItemNotFound && operation == "create" {
    var bytes = [UInt8](repeating: 0, count: 32)
    guard SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes) == errSecSuccess else { exit(70) }
    var item = query
    item[kSecValueData as String] = Data(bytes)
    item[kSecAttrLabel as String] = "LINEJS research session encryption key"
    let added = SecItemAdd(item as CFDictionary, nil)
    guard added == errSecSuccess || added == errSecDuplicateItem else { exit(71) }
    (status, key) = readKey()
}
if status == errSecItemNotFound { exit(44) }
guard status == errSecSuccess, let key = key, key.count == 32 else { exit(71) }
FileHandle.standardOutput.write(Data(key.base64EncodedString().utf8))
