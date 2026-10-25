import AppKit
import ApplicationServices
import CoreMedia
import CoreVideo
import Darwin
import IOKit.pwr_mgt
import ScreenCaptureKit
import VideoToolbox

// A one-viewer, no-audio desktop encoder. Stdout is a stream of big-endian
// uint32 length + Annex-B H.264 access unit. The supervising Go process owns
// network access and terminates this helper when the short-lived session ends.
@main
struct BridgeRemoteStream {
    static func main() async {
        do {
            guard CGPreflightScreenCaptureAccess() else { throw StreamError.permissionRequired }
            let displaySleepGuard = try DisplaySleepGuard()
            defer { displaySleepGuard.release() }
            // A sleeping panel can briefly appear in the shareable-content
            // list, yet SCStream stops after its first frame. A user explicitly
            // started this remote session: light the panel before creating the
            // stream, then keep it awake only for this helper's lifetime.
            var wakeAssertionID = IOPMAssertionID(0)
            let wakeStatus = IOPMAssertionDeclareUserActivity(
                "Bridge remote stream starting" as CFString, kIOPMUserActiveLocal,
                &wakeAssertionID)
            if wakeStatus != kIOReturnSuccess {
                throw StreamError.displayAssertionUnavailable(wakeStatus)
            }
            try await Task.sleep(for: .milliseconds(500))
            var content = try await SCShareableContent.excludingDesktopWindows(
                false, onScreenWindowsOnly: false)
            if !content.displays.contains(where: { $0.displayID == CGMainDisplayID() }) {
                var assertionID = IOPMAssertionID(0)
                if IOPMAssertionDeclareUserActivity(
                    "Bridge remote stream" as CFString, kIOPMUserActiveRemote, &assertionID) == kIOReturnSuccess {
                    try await Task.sleep(for: .milliseconds(500))
                    content = try await SCShareableContent.excludingDesktopWindows(
                        false, onScreenWindowsOnly: false)
                }
            }
            guard let display = content.displays.first(where: { $0.displayID == CGMainDisplayID() }) else {
                throw StreamError.noDisplay
            }
            let scale = min(1.0, 1920.0 / Double(max(1, display.width)))
            let width = max(1, Int(Double(display.width) * scale))
            let height = max(1, Int(Double(display.height) * scale))
            let output = try outputHandle()
            let encoder = try H264Encoder(width: width, height: height, output: output)
            let configuration = SCStreamConfiguration()
            configuration.width = width
            configuration.height = height
            configuration.showsCursor = true
            configuration.minimumFrameInterval = CMTime(value: 1, timescale: 10)
            configuration.queueDepth = 3
            let filter = SCContentFilter(display: display, excludingWindows: [])
            let stream = SCStream(filter: filter, configuration: configuration, delegate: encoder)
            try stream.addStreamOutput(encoder, type: .screen,
                                       sampleHandlerQueue: DispatchQueue(label: "bridge.remote.capture"))
            try await stream.startCapture()
            while true { try await Task.sleep(for: .seconds(1)) }
        } catch {
            fputs("bridge-remote-stream: \(error)\n", stderr)
            exit(1)
        }
    }

    private static func outputHandle() throws -> FileHandle {
        if CommandLine.arguments.count == 1 { return .standardOutput }
        guard CommandLine.arguments.count == 3,
              CommandLine.arguments[1] == "--socket" else { throw StreamError.invalidArguments }
        let path = CommandLine.arguments[2]
        let bytes = Array(path.utf8CString).map { UInt8(bitPattern: $0) }
        var address = sockaddr_un()
        guard !bytes.isEmpty, bytes.count <= MemoryLayout.size(ofValue: address.sun_path) else {
            throw StreamError.invalidArguments
        }
        address.sun_family = sa_family_t(AF_UNIX)
        withUnsafeMutableBytes(of: &address.sun_path) { $0.copyBytes(from: bytes) }
        let fd = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { throw StreamError.socketUnavailable }
        let result = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        if result != 0 {
            Darwin.close(fd)
            throw StreamError.socketUnavailable
        }
        return FileHandle(fileDescriptor: fd, closeOnDealloc: true)
    }
}

private final class DisplaySleepGuard {
    private var assertionID = IOPMAssertionID(0)

    init() throws {
        let status = IOPMAssertionCreateWithName(
            kIOPMAssertionTypePreventUserIdleDisplaySleep as CFString,
            IOPMAssertionLevel(kIOPMAssertionLevelOn),
            "Bridge remote stream active" as CFString, &assertionID)
        if status != kIOReturnSuccess { throw StreamError.displayAssertionUnavailable(status) }
    }

    func release() {
        if assertionID != 0 {
            IOPMAssertionRelease(assertionID)
            assertionID = 0
        }
    }

    deinit { release() }
}

private final class H264Encoder: NSObject, SCStreamOutput, SCStreamDelegate {
    private var compression: VTCompressionSession?
    private let output: FileHandle

    init(width: Int, height: Int, output: FileHandle) throws {
        self.output = output
        super.init()
        var session: VTCompressionSession?
        let status = VTCompressionSessionCreate(
            allocator: kCFAllocatorDefault, width: Int32(width), height: Int32(height),
            codecType: kCMVideoCodecType_H264, encoderSpecification: nil,
            imageBufferAttributes: nil, compressedDataAllocator: nil,
            outputCallback: { refcon, _, status, _, sampleBuffer in
                guard status == noErr, let refcon, let sampleBuffer else { return }
                Unmanaged<H264Encoder>.fromOpaque(refcon).takeUnretainedValue().emit(sampleBuffer)
            },
            refcon: Unmanaged.passUnretained(self).toOpaque(),
            compressionSessionOut: &session)
        guard status == noErr, let session else { throw StreamError.encoderUnavailable(status) }
        compression = session
        VTSessionSetProperty(session, key: kVTCompressionPropertyKey_RealTime, value: kCFBooleanTrue)
        VTSessionSetProperty(session, key: kVTCompressionPropertyKey_ProfileLevel,
                             value: kVTProfileLevel_H264_Baseline_AutoLevel)
        VTSessionSetProperty(session, key: kVTCompressionPropertyKey_AverageBitRate,
                             value: NSNumber(value: 2_000_000))
        VTSessionSetProperty(session, key: kVTCompressionPropertyKey_ExpectedFrameRate,
                             value: NSNumber(value: 10))
        VTSessionSetProperty(session, key: kVTCompressionPropertyKey_MaxKeyFrameInterval,
                             value: NSNumber(value: 10))
        VTCompressionSessionPrepareToEncodeFrames(session)
    }

    deinit {
        if let compression { VTCompressionSessionInvalidate(compression) }
    }

    func stream(_ stream: SCStream, didOutputSampleBuffer sampleBuffer: CMSampleBuffer,
                of outputType: SCStreamOutputType) {
        guard outputType == .screen, sampleBuffer.isValid,
              let image = CMSampleBufferGetImageBuffer(sampleBuffer), let compression else { return }
        let presentationTime = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
        let status = VTCompressionSessionEncodeFrame(
            compression, imageBuffer: image, presentationTimeStamp: presentationTime,
            duration: CMTime(value: 1, timescale: 10), frameProperties: nil,
            sourceFrameRefcon: nil, infoFlagsOut: nil)
        if status != noErr { fputs("bridge-remote-stream: encode error \(status)\n", stderr) }
    }

    func stream(_ stream: SCStream, didStopWithError error: Error) {
        fputs("bridge-remote-stream: capture stopped: \(error)\n", stderr)
        exit(1)
    }

    private func emit(_ sample: CMSampleBuffer) {
        guard CMSampleBufferDataIsReady(sample),
              let format = CMSampleBufferGetFormatDescription(sample),
              let block = CMSampleBufferGetDataBuffer(sample) else { return }
        var accessUnit = Data()
        for index in 0..<2 {
            var pointer: UnsafePointer<UInt8>?
            var size = 0
            let status = CMVideoFormatDescriptionGetH264ParameterSetAtIndex(
                format, parameterSetIndex: index, parameterSetPointerOut: &pointer,
                parameterSetSizeOut: &size, parameterSetCountOut: nil,
                nalUnitHeaderLengthOut: nil)
            if status == noErr, let pointer {
                accessUnit.append(contentsOf: [0, 0, 0, 1])
                accessUnit.append(pointer, count: size)
            }
        }
        let length = CMBlockBufferGetDataLength(block)
        guard length > 0, length < 4_000_000 else { return }
        var bytes = [UInt8](repeating: 0, count: length)
        let copyStatus = bytes.withUnsafeMutableBytes { buffer in
            CMBlockBufferCopyDataBytes(block, atOffset: 0, dataLength: length,
                                       destination: buffer.baseAddress!)
        }
        guard copyStatus == noErr else { return }
        var offset = 0
        while offset + 4 <= bytes.count {
            let size = (Int(bytes[offset]) << 24) | (Int(bytes[offset + 1]) << 16) |
                       (Int(bytes[offset + 2]) << 8) | Int(bytes[offset + 3])
            offset += 4
            guard size > 0, offset + size <= bytes.count else { return }
            accessUnit.append(contentsOf: [0, 0, 0, 1])
            accessUnit.append(contentsOf: bytes[offset..<(offset + size)])
            offset += size
        }
        guard !accessUnit.isEmpty, accessUnit.count < 4_000_000 else { return }
        let count = UInt32(accessUnit.count)
        let header = Data([UInt8((count >> 24) & 0xff), UInt8((count >> 16) & 0xff),
                           UInt8((count >> 8) & 0xff), UInt8(count & 0xff)])
        output.write(header)
        output.write(accessUnit)
    }
}

private enum StreamError: Error, CustomStringConvertible {
    case permissionRequired
    case noDisplay
    case encoderUnavailable(OSStatus)
    case displayAssertionUnavailable(IOReturn)
    case invalidArguments
    case socketUnavailable

    var description: String {
        switch self {
        case .permissionRequired: return "screen recording permission required"
        case .noDisplay: return "no active display"
        case .encoderUnavailable(let code): return "H.264 encoder unavailable: \(code)"
        case .displayAssertionUnavailable(let code): return "display assertion unavailable: \(code)"
        case .invalidArguments: return "invalid stream arguments"
        case .socketUnavailable: return "private stream socket unavailable"
        }
    }
}
