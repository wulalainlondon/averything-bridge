import AppKit
import ApplicationServices
import Carbon.HIToolbox
import CoreGraphics
import IOKit.pwr_mgt
import ScreenCaptureKit

// This helper is bundled with the signed macOS Bridge. It never listens on a
// socket: the paired, Tailscale-only Bridge endpoint decides whether to invoke
// a one-shot capture or a user-initiated click.
@main
struct BridgeRemoteHelper {
    static func main() async {
        do {
            guard CommandLine.arguments.count >= 2 else {
                throw HelperError.invalidCommand
            }
            switch CommandLine.arguments[1] {
            case "status":
                let payload: [String: Any] = [
                    "screenRecording": CGPreflightScreenCaptureAccess(),
                    "accessibility": AXIsProcessTrusted(),
                ]
                let data = try JSONSerialization.data(withJSONObject: payload)
                FileHandle.standardOutput.write(data)
            case "capture":
                try await capture()
            case "click":
                guard (4...5).contains(CommandLine.arguments.count),
                      let x = Double(CommandLine.arguments[2]),
                      let y = Double(CommandLine.arguments[3]) else {
                    throw HelperError.invalidCommand
                }
                let button = CommandLine.arguments.count == 5 ? CommandLine.arguments[4] : "left"
                try click(x: x, y: y, button: button)
            case "pointer":
                guard CommandLine.arguments.count == 5,
                      let x = Double(CommandLine.arguments[3]),
                      let y = Double(CommandLine.arguments[4]) else {
                    throw HelperError.invalidCommand
                }
                try pointer(action: CommandLine.arguments[2], x: x, y: y)
            case "text":
                guard CommandLine.arguments.count == 2,
                      let text = String(data: FileHandle.standardInput.readDataToEndOfFile(), encoding: .utf8) else {
                    throw HelperError.invalidCommand
                }
                try typeText(text)
            case "key":
                guard CommandLine.arguments.count == 3 else { throw HelperError.invalidCommand }
                try key(CommandLine.arguments[2])
            default:
                throw HelperError.invalidCommand
            }
        } catch {
            fputs("bridge-remote-helper: \(error)\n", stderr)
            exit(1)
        }
    }

    private static func capture() async throws {
        guard CGPreflightScreenCaptureAccess() else {
            throw HelperError.screenRecordingRequired
        }
        var content = try await SCShareableContent.excludingDesktopWindows(
            false, onScreenWindowsOnly: false)
        if !content.displays.contains(where: { $0.displayID == CGMainDisplayID() }) {
            // ScreenCaptureKit may report no display when the Mac panel has
            // idled off. A remote user has explicitly started this short-lived
            // session, so wake the panel once and retry discovery.
            var assertionID = IOPMAssertionID(0)
            let result = IOPMAssertionDeclareUserActivity(
                "Bridge remote desktop" as CFString, kIOPMUserActiveRemote, &assertionID)
            if result == kIOReturnSuccess {
                try await Task.sleep(for: .milliseconds(500))
                content = try await SCShareableContent.excludingDesktopWindows(
                    false, onScreenWindowsOnly: false)
            }
        }
        guard let display = content.displays.first(where: { $0.displayID == CGMainDisplayID() }) else {
            throw HelperError.noDisplay
        }
        let filter = SCContentFilter(display: display, excludingWindows: [])
        let configuration = SCStreamConfiguration()
        let scale = min(1.0, 1920.0 / Double(max(1, display.width)))
        configuration.width = max(1, Int(Double(display.width) * scale))
        configuration.height = max(1, Int(Double(display.height) * scale))
        configuration.showsCursor = true

        let image = try await SCScreenshotManager.captureImage(
            contentFilter: filter, configuration: configuration)
        guard let data = NSBitmapImageRep(cgImage: image).representation(
                using: .jpeg, properties: [.compressionFactor: 0.72]) else {
            throw HelperError.captureFailed
        }
        FileHandle.standardOutput.write(data)
    }

    private static func point(x: Double, y: Double) throws -> CGPoint {
        guard x.isFinite, y.isFinite, (0...1).contains(x), (0...1).contains(y) else {
            throw HelperError.invalidCoordinates
        }
        guard AXIsProcessTrusted() else {
            throw HelperError.accessibilityRequired
        }
        let bounds = CGDisplayBounds(CGMainDisplayID())
        guard bounds.width > 0, bounds.height > 0 else {
            throw HelperError.noDisplay
        }
        return CGPoint(x: bounds.minX + bounds.width * x,
                       y: bounds.minY + bounds.height * y)
    }

    private static func mouseEvent(_ type: CGEventType, at point: CGPoint,
                                   button: CGMouseButton = .left) throws {
        guard let event = CGEvent(mouseEventSource: nil, mouseType: type,
                                  mouseCursorPosition: point, mouseButton: button) else {
            throw HelperError.inputFailed
        }
        event.post(tap: .cghidEventTap)
    }

    private static func click(x: Double, y: Double, button: String) throws {
        guard button == "left" || button == "right" else { throw HelperError.invalidCommand }
        let position = try point(x: x, y: y)
        try mouseEvent(button == "right" ? .rightMouseDown : .leftMouseDown,
                       at: position, button: button == "right" ? .right : .left)
        usleep(30_000)
        try mouseEvent(button == "right" ? .rightMouseUp : .leftMouseUp,
                       at: position, button: button == "right" ? .right : .left)
    }

    private static func pointer(action: String, x: Double, y: Double) throws {
        let position = try point(x: x, y: y)
        let type: CGEventType
        switch action {
        case "move": type = .mouseMoved
        case "dragStart": type = .leftMouseDown
        case "dragMove": type = .leftMouseDragged
        case "dragEnd": type = .leftMouseUp
        default: throw HelperError.invalidCommand
        }
        try mouseEvent(type, at: position)
    }

    private static func typeText(_ text: String) throws {
        guard !text.isEmpty, text.utf8.count <= 2048, AXIsProcessTrusted() else {
            throw HelperError.invalidCommand
        }
        let system = AXUIElementCreateSystemWide()
        var focused: CFTypeRef?
        if AXUIElementCopyAttributeValue(system, kAXFocusedUIElementAttribute as CFString, &focused) == .success,
           let focused {
            let element = focused as! AXUIElement
            if AXUIElementSetAttributeValue(element, kAXSelectedTextAttribute as CFString,
                                            text as CFTypeRef) == .success {
                return
            }
        }
        let units = Array(text.utf16)
        guard let down = CGEvent(keyboardEventSource: nil, virtualKey: 0, keyDown: true),
              let up = CGEvent(keyboardEventSource: nil, virtualKey: 0, keyDown: false) else {
            throw HelperError.inputFailed
        }
        units.withUnsafeBufferPointer { buffer in
            down.keyboardSetUnicodeString(stringLength: buffer.count, unicodeString: buffer.baseAddress)
        }
        down.post(tap: .cgSessionEventTap)
        usleep(30_000)
        up.post(tap: .cgSessionEventTap)
        usleep(30_000)
    }

    private static func focusedTextElement() -> AXUIElement? {
        let system = AXUIElementCreateSystemWide()
        var focused: CFTypeRef?
        guard AXUIElementCopyAttributeValue(system, kAXFocusedUIElementAttribute as CFString, &focused) == .success,
              let focused else { return nil }
        return (focused as! AXUIElement)
    }

    private static func deleteSelectedOrPreviousCharacter(_ element: AXUIElement) -> Bool {
        var rawRange: CFTypeRef?
        guard AXUIElementCopyAttributeValue(element, kAXSelectedTextRangeAttribute as CFString, &rawRange) == .success,
              let rawRange else { return false }
        var range = CFRange(location: 0, length: 0)
        guard AXValueGetValue(rawRange as! AXValue, .cfRange, &range) else { return false }
        if range.length == 0 {
            guard range.location > 0 else { return true }
            var rawText: CFTypeRef?
            guard AXUIElementCopyAttributeValue(element, kAXValueAttribute as CFString, &rawText) == .success,
                  let value = rawText as? String else { return false }
            let previous = (value as NSString).rangeOfComposedCharacterSequence(at: range.location - 1)
            range = CFRange(location: previous.location, length: previous.length)
            guard let selection = AXValueCreate(.cfRange, &range),
                  AXUIElementSetAttributeValue(element, kAXSelectedTextRangeAttribute as CFString,
                                               selection) == .success else { return false }
        }
        return AXUIElementSetAttributeValue(element, kAXSelectedTextAttribute as CFString,
                                            "" as CFTypeRef) == .success
    }

    private static func moveTextCaret(_ element: AXUIElement, direction: String) -> Bool {
        var rawRange: CFTypeRef?
        var rawText: CFTypeRef?
        guard AXUIElementCopyAttributeValue(element, kAXSelectedTextRangeAttribute as CFString, &rawRange) == .success,
              AXUIElementCopyAttributeValue(element, kAXValueAttribute as CFString, &rawText) == .success,
              let rawRange, let value = rawText as? String else { return false }
        var range = CFRange(location: 0, length: 0)
        guard AXValueGetValue(rawRange as! AXValue, .cfRange, &range) else { return false }
        let text = value as NSString
        let location = max(0, min(text.length, range.location))
        var destination = location
        if direction == "ArrowLeft" {
            destination = range.length > 0 ? location :
                (location == 0 ? 0 : text.rangeOfComposedCharacterSequence(at: location - 1).location)
        } else if direction == "ArrowRight" {
            destination = range.length > 0 ? min(text.length, location + range.length) :
                (location == text.length ? text.length : NSMaxRange(text.rangeOfComposedCharacterSequence(at: location)))
        } else {
            let units = Array(value.utf16)
            func start(_ at: Int) -> Int {
                var cursor = min(at, units.count)
                while cursor > 0 && units[cursor - 1] != 10 { cursor -= 1 }
                return cursor
            }
            func end(_ from: Int) -> Int {
                var cursor = from
                while cursor < units.count && units[cursor] != 10 { cursor += 1 }
                return cursor
            }
            let currentStart = start(location)
            let column = location - currentStart
            if direction == "ArrowUp" {
                if currentStart > 0 {
                    let previousEnd = currentStart - 1
                    destination = min(start(previousEnd) + column, previousEnd)
                }
            } else if direction == "ArrowDown" {
                let currentEnd = end(currentStart)
                if currentEnd < units.count {
                    let nextStart = currentEnd + 1
                    destination = min(nextStart + column, end(nextStart))
                }
            } else { return false }
        }
        range = CFRange(location: destination, length: 0)
        guard let selection = AXValueCreate(.cfRange, &range) else { return false }
        return AXUIElementSetAttributeValue(element, kAXSelectedTextRangeAttribute as CFString,
                                            selection) == .success
    }

    private static func key(_ name: String) throws {
        guard AXIsProcessTrusted() else { throw HelperError.accessibilityRequired }
        if let element = focusedTextElement() {
            if name == "Enter" && AXUIElementSetAttributeValue(element,
                    kAXSelectedTextAttribute as CFString, "\n" as CFTypeRef) == .success { return }
            if name == "Tab" && AXUIElementSetAttributeValue(element,
                    kAXSelectedTextAttribute as CFString, "\t" as CFTypeRef) == .success { return }
            if name == "Backspace" && deleteSelectedOrPreviousCharacter(element) { return }
            if name.hasPrefix("Arrow") && moveTextCaret(element, direction: name) { return }
        }
        let code: CGKeyCode
        switch name {
        case "Enter": code = CGKeyCode(kVK_Return)
        case "Backspace": code = CGKeyCode(kVK_Delete)
        case "Tab": code = CGKeyCode(kVK_Tab)
        case "Escape": code = CGKeyCode(kVK_Escape)
        case "ArrowLeft": code = CGKeyCode(kVK_LeftArrow)
        case "ArrowRight": code = CGKeyCode(kVK_RightArrow)
        case "ArrowUp": code = CGKeyCode(kVK_UpArrow)
        case "ArrowDown": code = CGKeyCode(kVK_DownArrow)
        default: throw HelperError.invalidCommand
        }
        guard let down = CGEvent(keyboardEventSource: nil, virtualKey: code, keyDown: true),
              let up = CGEvent(keyboardEventSource: nil, virtualKey: code, keyDown: false) else {
            throw HelperError.inputFailed
        }
        let system = AXUIElementCreateSystemWide()
        var focused: CFTypeRef?
        if AXUIElementCopyAttributeValue(system, kAXFocusedUIElementAttribute as CFString, &focused) == .success,
           let focused {
            var pid = pid_t(0)
            if AXUIElementGetPid(focused as! AXUIElement, &pid) == .success && pid > 0 {
                down.postToPid(pid)
                usleep(30_000)
                up.postToPid(pid)
                usleep(30_000)
                return
            }
        }
        down.post(tap: .cgSessionEventTap)
        usleep(30_000)
        up.post(tap: .cgSessionEventTap)
        usleep(30_000)
    }
}

private enum HelperError: Error, CustomStringConvertible {
    case invalidCommand
    case invalidCoordinates
    case noDisplay
    case screenRecordingRequired
    case accessibilityRequired
    case captureFailed
    case inputFailed

    var description: String {
        switch self {
        case .invalidCommand: return "invalid command"
        case .invalidCoordinates: return "invalid coordinates"
        case .noDisplay: return "no active display"
        case .screenRecordingRequired: return "screen recording permission required"
        case .accessibilityRequired: return "accessibility permission required"
        case .captureFailed: return "screen capture failed"
        case .inputFailed: return "mouse event unavailable"
        }
    }
}
