// simulmt-audio-host — process/global Core Audio capture for SimulMT.
// stdout audio protocol: raw PCM S16LE, mono, 16 kHz.
// --list emits a JSON array of currently registered Core Audio processes.

import Foundation
import AppKit
import CoreAudio
import AudioToolbox
import AVFoundation

func elog(_ message: String) {
    FileHandle.standardError.write((message + "\n").data(using: .utf8)!)
}

func die(_ message: String) -> Never {
    elog("[audio-host] \(message)")
    exit(1)
}

func address(_ selector: AudioObjectPropertySelector,
             _ scope: AudioObjectPropertyScope = kAudioObjectPropertyScopeGlobal) -> AudioObjectPropertyAddress {
    AudioObjectPropertyAddress(mSelector: selector, mScope: scope, mElement: kAudioObjectPropertyElementMain)
}

let systemObject = AudioObjectID(kAudioObjectSystemObject)

func processObjectID(pid: pid_t) -> AudioObjectID? {
    var property = address(kAudioHardwarePropertyProcessObjectList)
    var size: UInt32 = 0
    guard AudioObjectGetPropertyDataSize(systemObject, &property, 0, nil, &size) == noErr else { return nil }
    let count = Int(size) / MemoryLayout<AudioObjectID>.size
    var list = [AudioObjectID](repeating: 0, count: count)
    guard AudioObjectGetPropertyData(systemObject, &property, 0, nil, &size, &list) == noErr else { return nil }
    for object in list {
        var property = address(kAudioProcessPropertyPID)
        var value: pid_t = 0
        var valueSize = UInt32(MemoryLayout<pid_t>.size)
        if AudioObjectGetPropertyData(object, &property, 0, nil, &valueSize, &value) == noErr, value == pid {
            return object
        }
    }
    return nil
}

func audioProcesses() -> [(pid: pid_t, object: AudioObjectID)] {
    var property = address(kAudioHardwarePropertyProcessObjectList)
    var size: UInt32 = 0
    guard AudioObjectGetPropertyDataSize(systemObject, &property, 0, nil, &size) == noErr else { return [] }
    let count = Int(size) / MemoryLayout<AudioObjectID>.size
    var list = [AudioObjectID](repeating: 0, count: count)
    guard AudioObjectGetPropertyData(systemObject, &property, 0, nil, &size, &list) == noErr else { return [] }
    var result: [(pid_t, AudioObjectID)] = []
    for object in list {
        var property = address(kAudioProcessPropertyPID)
        var pid: pid_t = 0
        var valueSize = UInt32(MemoryLayout<pid_t>.size)
        if AudioObjectGetPropertyData(object, &property, 0, nil, &valueSize, &pid) == noErr {
            result.append((pid, object))
        }
    }
    return result
}

func processIsRunning(_ object: AudioObjectID) -> Bool {
    var property = address(kAudioProcessPropertyIsRunning)
    var running: UInt32 = 0
    var size = UInt32(MemoryLayout<UInt32>.size)
    return AudioObjectGetPropertyData(object, &property, 0, nil, &size, &running) == noErr && running != 0
}

/// Chromium and other multi-process applications often route audio through a
/// helper PID rather than the application PID selected by the user. Audio
/// services do not always expose an NSRunningApplication or bundle identifier,
/// so resolve both bundle membership and the process hierarchy.
func parentProcessID(pid: pid_t) -> pid_t? {
    var info = kinfo_proc()
    var size = MemoryLayout<kinfo_proc>.size
    var mib: [Int32] = [CTL_KERN, KERN_PROC, KERN_PROC_PID, pid]
    guard sysctl(&mib, u_int(mib.count), &info, &size, nil, 0) == 0 else { return nil }
    return info.kp_eproc.e_ppid
}

func applicationAudioPIDs(roots: [pid_t]) -> [pid_t] {
    guard !roots.isEmpty else { return [] }
    let rootBundles = roots.compactMap { NSRunningApplication(processIdentifier: $0)?.bundleIdentifier }
    let registeredPIDs = audioProcesses().map(\.pid)
    var accepted = Set(roots)
    var changed = true
    while changed {
        changed = false
        for pid in registeredPIDs where !accepted.contains(pid) {
            let bundleMatches = NSRunningApplication(processIdentifier: pid)?.bundleIdentifier.map { bundle in
                rootBundles.contains(where: { bundle == $0 || bundle.hasPrefix($0 + ".") })
            } ?? false
            var ancestor = parentProcessID(pid: pid)
            var descendantMatches = false
            var depth = 0
            while let parent = ancestor, parent > 1, depth < 16 {
                if accepted.contains(parent) {
                    descendantMatches = true
                    break
                }
                ancestor = parentProcessID(pid: parent)
                depth += 1
            }
            if bundleMatches || descendantMatches {
                accepted.insert(pid)
                changed = true
            }
        }
    }
    return registeredPIDs.filter { accepted.contains($0) }.sorted()
}

if CommandLine.arguments.contains("--list") {
    var seen = Set<pid_t>()
    var items: [[String: Any]] = []
    for (pid, object) in audioProcesses() where !seen.contains(pid) {
        seen.insert(pid)
        guard let app = NSRunningApplication(processIdentifier: pid) else { continue }
        items.append([
            "pid": Int(pid),
            "name": app.localizedName ?? "Application \(pid)",
            "bundle_id": app.bundleIdentifier as Any,
            "is_running": processIsRunning(object)
        ])
    }
    let data = try! JSONSerialization.data(withJSONObject: items, options: [.sortedKeys])
    FileHandle.standardOutput.write(data)
    exit(0)
}

func defaultOutputDevice() -> AudioObjectID {
    var property = address(kAudioHardwarePropertyDefaultOutputDevice)
    var device = AudioObjectID(0)
    var size = UInt32(MemoryLayout<AudioObjectID>.size)
    let error = AudioObjectGetPropertyData(systemObject, &property, 0, nil, &size, &device)
    guard error == noErr, device != 0 else { die("cannot get default output device: \(error)") }
    return device
}

func deviceUID(_ device: AudioObjectID) -> String {
    var property = address(kAudioDevicePropertyDeviceUID)
    var value: CFString?
    var size = UInt32(MemoryLayout<CFString?>.size)
    let error = withUnsafeMutablePointer(to: &value) {
        AudioObjectGetPropertyData(device, &property, 0, nil, &size, $0)
    }
    guard error == noErr, let value else { die("cannot get output device UID: \(error)") }
    return value as String
}

func inputFormat(_ device: AudioObjectID) -> AudioStreamBasicDescription? {
    var property = address(kAudioDevicePropertyStreamFormat, kAudioObjectPropertyScopeInput)
    var description = AudioStreamBasicDescription()
    var size = UInt32(MemoryLayout<AudioStreamBasicDescription>.size)
    let error = AudioObjectGetPropertyData(device, &property, 0, nil, &size, &description)
    return error == noErr ? description : nil
}

var excludePIDs: [pid_t] = []
var includePIDs: [pid_t] = []
var muted = true
var seconds: Double = 0
var outPath: String?
var verbose = false

var index = 1
let arguments = CommandLine.arguments
while index < arguments.count {
    switch arguments[index] {
    case "--exclude-pid":
        index += 1
        if index < arguments.count, let value = pid_t(arguments[index]) { excludePIDs.append(value) }
    case "--pid":
        index += 1
        if index < arguments.count, let value = pid_t(arguments[index]) { includePIDs.append(value) }
    case "--no-mute": muted = false
    case "--verbose": verbose = true
    case "--seconds":
        index += 1
        if index < arguments.count { seconds = Double(arguments[index]) ?? 0 }
    case "--out":
        index += 1
        if index < arguments.count { outPath = arguments[index] }
    default: break
    }
    index += 1
}

if arguments.contains("--microphone") {
    runMicrophone(outPath: outPath)
}

let excludeObjects = excludePIDs.compactMap { pid -> AudioObjectID? in
    let object = processObjectID(pid: pid)
    if object == nil { elog("[audio-host] warning: playback pid=\(pid) is not registered yet") }
    return object
}
let capturedApplicationPIDs = applicationAudioPIDs(roots: includePIDs)
let includeObjects = capturedApplicationPIDs.compactMap { processObjectID(pid: $0) }
if !includePIDs.isEmpty && includeObjects.isEmpty {
    die("application audio is not ready; start playback and select the application again")
}

let tapDescription: CATapDescription
if includePIDs.isEmpty {
    tapDescription = CATapDescription(stereoGlobalTapButExcludeProcesses: excludeObjects)
    tapDescription.isExclusive = true
    elog("[audio-host] mode=global exclude=\(excludeObjects.count)")
} else {
    tapDescription = CATapDescription(stereoMixdownOfProcesses: includeObjects)
    tapDescription.isExclusive = false
    elog("[audio-host] mode=process roots=\(includePIDs) captured=\(capturedApplicationPIDs)")
}
tapDescription.uuid = UUID()
tapDescription.muteBehavior = muted ? .mutedWhenTapped : .unmuted
tapDescription.isPrivate = true
tapDescription.name = "simulmt-audio-tap"

var tapID = AudioObjectID(kAudioObjectUnknown)
let tapError = AudioHardwareCreateProcessTap(tapDescription, &tapID)
guard tapError == noErr else {
    die("cannot create process tap (\(tapError)); grant Screen & System Audio Recording permission")
}

let outputDevice = defaultOutputDevice()
let aggregateUID = "simulmt-tap-\(UUID().uuidString)"
let aggregateDescription: [String: Any] = [
    kAudioAggregateDeviceNameKey: "simulmt-tap",
    kAudioAggregateDeviceUIDKey: aggregateUID,
    kAudioAggregateDeviceMainSubDeviceKey: deviceUID(outputDevice),
    kAudioAggregateDeviceIsPrivateKey: true,
    kAudioAggregateDeviceIsStackedKey: false,
    kAudioAggregateDeviceTapAutoStartKey: true,
    kAudioAggregateDeviceSubDeviceListKey: [[kAudioSubDeviceUIDKey: deviceUID(outputDevice)]],
    kAudioAggregateDeviceTapListKey: [[
        kAudioSubTapUIDKey: tapDescription.uuid.uuidString,
        kAudioSubTapDriftCompensationKey: true
    ]]
]
var aggregateID = AudioObjectID(kAudioObjectUnknown)
let aggregateError = AudioHardwareCreateAggregateDevice(aggregateDescription as CFDictionary, &aggregateID)
guard aggregateError == noErr, aggregateID != 0 else { die("cannot create aggregate device: \(aggregateError)") }

usleep(200_000)
let sourceRate = inputFormat(aggregateID)?.mSampleRate ?? 48_000
elog("[audio-host] source_rate=\(Int(sourceRate)) target=16000 mono_s16le mute=\(muted)")

var retainedHandle: FileHandle?
let outputFD: Int32
if let outPath {
    FileManager.default.createFile(atPath: outPath, contents: nil)
    guard let handle = FileHandle(forWritingAtPath: outPath) else { die("cannot open output file") }
    retainedHandle = handle
    outputFD = handle.fileDescriptor
} else {
    outputFD = FileHandle.standardOutput.fileDescriptor
}

let targetRate = 16_000.0
let step = sourceRate / targetRate
var resamplePosition = 0.0
var lastSample: Float = 0
var mono = [Float](repeating: 0, count: 16_384)
var outputBuffer = [Int16](repeating: 0, count: 8_192)
var ioCalls = 0
var totalFrames: Int64 = 0

func emit(_ base: UnsafeRawPointer, _ count: Int) {
    var offset = 0
    while offset < count {
        let written = write(outputFD, base.advanced(by: offset), count - offset)
        if written <= 0 { return }
        offset += written
    }
}

func onIO(_ now: UnsafePointer<AudioTimeStamp>,
          _ inputData: UnsafePointer<AudioBufferList>,
          _ inputTime: UnsafePointer<AudioTimeStamp>,
          _ outputData: UnsafeMutablePointer<AudioBufferList>,
          _ outputTime: UnsafePointer<AudioTimeStamp>) {
    let buffers = UnsafeMutableAudioBufferListPointer(UnsafeMutablePointer(mutating: inputData))
    ioCalls += 1
    guard !buffers.isEmpty else { return }
    var frames = 0
    if buffers.count == 1, let data = buffers[0].mData {
        let channels = Int(buffers[0].mNumberChannels)
        let count = Int(buffers[0].mDataByteSize) / MemoryLayout<Float>.size
        let samples = data.assumingMemoryBound(to: Float.self)
        if channels <= 1 {
            frames = min(count, mono.count)
            for i in 0..<frames { mono[i] = samples[i] }
        } else {
            frames = min(count / channels, mono.count)
            for i in 0..<frames {
                var sum: Float = 0
                for channel in 0..<channels { sum += samples[i * channels + channel] }
                mono[i] = sum / Float(channels)
            }
        }
    } else {
        let firstCount = Int(buffers[0].mDataByteSize) / MemoryLayout<Float>.size
        frames = min(firstCount, mono.count)
        guard frames > 0 else { return }
        for i in 0..<frames { mono[i] = 0 }
        var used = 0
        for buffer in buffers {
            guard let data = buffer.mData else { continue }
            let count = min(Int(buffer.mDataByteSize) / MemoryLayout<Float>.size, frames)
            let samples = data.assumingMemoryBound(to: Float.self)
            for i in 0..<count { mono[i] += samples[i] }
            used += 1
        }
        if used > 1 { for i in 0..<frames { mono[i] /= Float(used) } }
    }
    guard frames > 0 else { return }
    var outputCount = 0
    while resamplePosition < Double(frames - 1), outputCount < outputBuffer.count {
        let position = Int(resamplePosition)
        let fraction = Float(resamplePosition - Double(position))
        let a = position > 0 ? mono[position] : lastSample
        let b = mono[position + 1]
        let sample = max(-1, min(1, a + (b - a) * fraction))
        outputBuffer[outputCount] = Int16(sample * 32_767)
        outputCount += 1
        resamplePosition += step
    }
    resamplePosition -= Double(frames)
    lastSample = mono[frames - 1]
    guard outputCount > 0 else { return }
    totalFrames += Int64(outputCount)
    outputBuffer.withUnsafeBytes { bytes in
        emit(bytes.baseAddress!, outputCount * MemoryLayout<Int16>.size)
    }
}

var procID: AudioDeviceIOProcID?
let procError = AudioDeviceCreateIOProcIDWithBlock(&procID, aggregateID, nil, onIO)
guard procError == noErr, let createdProcID = procID else { die("cannot install IOProc: \(procError)") }
let startError = AudioDeviceStart(aggregateID, createdProcID)
guard startError == noErr else { die("cannot start aggregate device: \(startError)") }
elog("[audio-host] capturing")

let teardownLock = NSLock()
var tornDown = false
func teardown(_ reason: String) {
    teardownLock.lock()
    if tornDown {
        teardownLock.unlock()
        return
    }
    tornDown = true
    teardownLock.unlock()
    AudioDeviceStop(aggregateID, procID!)
    AudioDeviceDestroyIOProcID(aggregateID, procID!)
    AudioHardwareDestroyAggregateDevice(aggregateID)
    AudioHardwareDestroyProcessTap(tapID)
    elog("[audio-host] \(reason); original audio restored; frames=\(totalFrames)")
}

signal(SIGINT) { _ in
    teardown("interrupted")
    exit(0)
}
signal(SIGTERM) { _ in
    teardown("terminated")
    exit(0)
}

let capturedDeviceUID = deviceUID(outputDevice)
var outputDeviceAddress = address(kAudioHardwarePropertyDefaultOutputDevice)
var outputDeviceListener: AudioObjectPropertyListenerBlock = { _, _ in
    DispatchQueue.global().asyncAfter(deadline: .now() + 0.2) {
        if deviceUID(defaultOutputDevice()) != capturedDeviceUID {
            teardown("output device changed")
            exit(4)
        }
    }
}
AudioObjectAddPropertyListenerBlock(systemObject, &outputDeviceAddress, DispatchQueue.global(), outputDeviceListener)

func monitorSourceProcesses() {
    if !includePIDs.isEmpty {
        let alive = includePIDs.allSatisfy { kill($0, 0) == 0 }
        if !alive {
            teardown("source application exited")
            exit(3)
        }
        if applicationAudioPIDs(roots: includePIDs) != capturedApplicationPIDs {
            teardown("application audio processes changed; reconnecting")
            exit(5)
        }
    }
    DispatchQueue.global().asyncAfter(deadline: .now() + 1) {
        monitorSourceProcesses()
    }
}
monitorSourceProcesses()

if seconds > 0 {
    DispatchQueue.global().asyncAfter(deadline: .now() + seconds) {
        teardown("capture finished")
        exit(0)
    }
}
while true { sleep(1) }

func runMicrophone(outPath: String?) -> Never {
    if AVCaptureDevice.authorizationStatus(for: .audio) == .notDetermined {
        let semaphore = DispatchSemaphore(value: 0)
        AVCaptureDevice.requestAccess(for: .audio) { _ in semaphore.signal() }
        semaphore.wait()
    }
    guard AVCaptureDevice.authorizationStatus(for: .audio) == .authorized else {
        die("microphone permission denied; grant access to the terminal or host application")
    }
    let engine = AVAudioEngine()
    let input = engine.inputNode
    let inputFormat = input.outputFormat(forBus: 0)
    guard inputFormat.sampleRate > 0, inputFormat.channelCount > 0,
          let targetFormat = AVAudioFormat(commonFormat: .pcmFormatInt16, sampleRate: 16_000, channels: 1, interleaved: true),
          let converter = AVAudioConverter(from: inputFormat, to: targetFormat) else {
        die("microphone format is unavailable")
    }
    let descriptor = outPath.map { path -> (Int32, FileHandle?) in
        FileManager.default.createFile(atPath: path, contents: nil)
        guard let handle = FileHandle(forWritingAtPath: path) else { die("cannot open microphone output") }
        return (handle.fileDescriptor, handle)
    } ?? (FileHandle.standardOutput.fileDescriptor, nil)
    _ = descriptor.1
    input.installTap(onBus: 0, bufferSize: 1_600, format: inputFormat) { buffer, _ in
        let capacity = AVAudioFrameCount(Double(buffer.frameLength) * 16_000 / inputFormat.sampleRate) + 1_024
        guard let converted = AVAudioPCMBuffer(pcmFormat: targetFormat, frameCapacity: capacity) else { return }
        var consumed = false
        var conversionError: NSError?
        converter.convert(to: converted, error: &conversionError) { _, status in
            if consumed {
                status.pointee = .noDataNow
                return nil
            }
            consumed = true
            status.pointee = .haveData
            return buffer
        }
        guard conversionError == nil, converted.frameLength > 0,
              let samples = converted.int16ChannelData else { return }
        let bytes = Data(bytes: samples[0], count: Int(converted.frameLength) * MemoryLayout<Int16>.size)
        bytes.withUnsafeBytes { raw in
            guard let base = raw.baseAddress else { return }
            var offset = 0
            while offset < raw.count {
                let written = write(descriptor.0, base.advanced(by: offset), raw.count - offset)
                if written <= 0 { return }
                offset += written
            }
        }
    }
    engine.prepare()
    do {
        try engine.start()
    } catch {
        die("cannot start microphone: \(error.localizedDescription)")
    }
    elog("[audio-host] mode=microphone target=16000 mono_s16le")
    signal(SIGINT) { _ in exit(0) }
    signal(SIGTERM) { _ in exit(0) }
    RunLoop.main.run()
    exit(0)
}
