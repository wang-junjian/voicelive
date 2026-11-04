// simulmt-tts-host — persistent, independently identifiable WAV player.
// stdin/stdout: newline-delimited JSON. It never shares the captured browser process.
//
// Continuous playback:
//   - synthesize ahead and schedule the next file at the exact end of the
//     previous one instead of waiting for its completion callback;
//   - realtime: aggressive source fitting and catch-up, with stale-speech skip;
//   - balanced: narrow 0.85–1.25x pacing and a higher skip threshold;
//   - fidelity: fixed 1.0x pacing, never speed up, slow down, or skip speech.
// Subtitles already delivered upstream are unaffected by speech skipping.

import Foundation
import AVFoundation

struct Command: Decodable {
    let cmd: String
    let token: UInt64?
    let path: String?
    let text: String?
    let voice: String?
    let rate: UInt32?
    let source_duration_ms: Double?
    let latency_mode: String?
}

private struct Item {
    let token: UInt64
    let player: AVAudioPlayer
    let duration: TimeInterval
    let targetDuration: TimeInterval
    var startTime: TimeInterval = 0
    var rate: Float = 1
    var announced = false
}

private let backlogFaster: TimeInterval = 8
private let backlogSkip: TimeInterval = 15
private let rateFast: Float = 1.06
private let rateFaster: Float = 1.12
private let initialBufferDuration: TimeInterval = 3.2
private let maximumInitialWait: TimeInterval = 1.2

final class Player: NSObject, AVAudioPlayerDelegate {
    private var pending: [Item] = []
    private var active: [Item] = []
    private var synthesisDepth = 0
    private var paused = false
    private var hasStartedSpeech = false
    private var firstSpeechReadyAt: TimeInterval?
    private var nextStartTime: TimeInterval = 0
    private var latencyMode = "balanced"
    private var timer: Timer?

    override init() {
        super.init()
        timer = Timer.scheduledTimer(withTimeInterval: 0.05, repeats: true) { [weak self] _ in
            guard let self, !self.paused else { return }
            self.announceStarts()
            self.adjustForBacklog()
            self.pump()
        }
    }

    func emit(_ type: String, token: UInt64? = nil, message: String? = nil, depth: Int? = nil, rate: Float? = nil, startTime: TimeInterval? = nil) {
        var object: [String: Any] = ["type": type]
        if let token { object["token"] = token }
        if let message { object["message"] = message }
        if let depth { object["depth"] = depth }
        if let rate { object["rate"] = rate }
        if let startTime { object["start_time"] = startTime }
        if let data = try? JSONSerialization.data(withJSONObject: object, options: [.sortedKeys]),
           let line = String(data: data, encoding: .utf8) {
            print(line)
            fflush(stdout)
        }
    }

    func setSynthesisDepth(_ depth: Int) {
        synthesisDepth = depth
        reportDepth()
    }

    private func reportDepth() {
        emit("queue", depth: synthesisDepth + pending.count + active.count)
    }

    func enqueue(token: UInt64, path: String, sourceDurationMs: Double?) {
        do {
            let url = URL(fileURLWithPath: path)
            let player = try AVAudioPlayer(contentsOf: url)
            player.delegate = self
            player.enableRate = true
            player.prepareToPlay()
            let targetDuration = max((sourceDurationMs ?? 0) / 1000, 0)
            pending.append(Item(token: token, player: player, duration: player.duration, targetDuration: targetDuration))
            if token != 0 && targetDuration > 0 && firstSpeechReadyAt == nil {
                firstSpeechReadyAt = Date().timeIntervalSinceReferenceDate
            }
            reportDepth()
            adjustForBacklog()
            pump()
        } catch {
            emit("error", token: token, message: error.localizedDescription)
        }
    }

    func handle(_ command: Command) {
        DispatchQueue.main.async {
            switch command.cmd {
            case "speak":
                break
            case "policy":
                if let mode = command.latency_mode { self.latencyMode = mode }
            case "play":
                guard let token = command.token, let path = command.path else { return }
                self.enqueue(token: token, path: path, sourceDurationMs: command.source_duration_ms)
            case "pause":
                self.paused = true
                for item in self.active { item.player.pause() }
            case "resume":
                self.resumeScheduledPlayback()
            case "flush":
                let cancelled = self.pending.map { $0.token } + self.active.map { $0.token }
                self.pending.removeAll()
                for item in self.active { item.player.stop() }
                self.active.removeAll()
                self.paused = false
                self.hasStartedSpeech = false
                self.firstSpeechReadyAt = nil
                self.nextStartTime = 0
                for token in cancelled { self.emit("cancelled", token: token) }
                self.reportDepth()
            case "stop":
                self.pending.removeAll()
                for item in self.active { item.player.stop() }
                exit(0)
            default:
                self.emit("error", message: "unknown command: \(command.cmd)")
            }
        }
    }

    private func pump() {
        guard !paused, !pending.isEmpty, active.count < 2 else { return }
        if !hasStartedSpeech && pending.first?.token != 0 {
            let buffered = pending.reduce(0) { $0 + $1.targetDuration }
            let waited = firstSpeechReadyAt.map { Date().timeIntervalSinceReferenceDate - $0 } ?? 0
            let direct = pending.contains { $0.targetDuration == 0 }
            let initialBuffer: TimeInterval = latencyMode == "low_latency" ? 0.5 : (latencyMode == "complete" ? 2.4 : initialBufferDuration)
            let maximumWait: TimeInterval = latencyMode == "low_latency" ? 0.25 : (latencyMode == "complete" ? 2.0 : maximumInitialWait)
            guard direct || pending.count >= 2 || buffered >= initialBuffer || waited >= maximumWait else { return }
            hasStartedSpeech = true
            firstSpeechReadyAt = nil
        }
        while !paused && !pending.isEmpty && active.count < 2 {
            var item = pending.removeFirst()
            let player = item.player
            let fit = item.targetDuration > 0.05 ? item.duration / item.targetDuration : 1.0
            switch latencyMode {
            case "low_latency":
                item.rate = Float(min(max(fit * catchupMultiplier(), 0.75), 1.8))
            case "complete":
                item.rate = 1.0
            default:
                item.rate = Float(min(max(fit * catchupMultiplier(), 0.85), 1.25))
            }
            player.rate = item.rate
            let now = player.deviceCurrentTime
            let start = max(nextStartTime, now + 0.03)
            guard player.play(atTime: start) else {
                emit("error", token: item.token, message: "AVAudioPlayer failed to schedule")
                reportDepth()
                continue
            }
            item.startTime = start
            nextStartTime = start + item.duration / TimeInterval(item.rate)
            active.append(item)
            emit("scheduled", token: item.token, rate: item.rate, startTime: start)
            reportDepth()
            if !hasStartedSpeech && pending.isEmpty { break }
        }
    }

    private func announceStarts() {
        for index in active.indices {
            let item = active[index]
            if item.token == 0 || item.announced { continue }
            if item.player.deviceCurrentTime + 0.02 >= item.startTime {
                active[index].announced = true
                emit("started", token: item.token)
            }
        }
    }

    private func resumeScheduledPlayback() {
        paused = false
        guard !active.isEmpty else {
            pump()
            return
        }
        var start = active[0].player.deviceCurrentTime + 0.05
        for index in active.indices {
            var item = active[index]
            item.startTime = start
            if !item.player.play(atTime: start) {
                emit("error", token: item.token, message: "AVAudioPlayer failed to resume")
            }
            start += max(item.duration - item.player.currentTime, 0) / TimeInterval(item.rate)
            active[index] = item
        }
        nextStartTime = start
        pump()
    }

    func audioPlayerDidFinishPlaying(_ player: AVAudioPlayer, successfully flag: Bool) {
        guard let index = active.firstIndex(where: { $0.player === player }) else { return }
        let item = active.remove(at: index)
        emit("done", token: item.token)
        reportDepth()
        pump()
    }

    private func backlog() -> TimeInterval {
        var total: TimeInterval = 0
        for item in active {
            total += max(item.duration - item.player.currentTime, 0) / TimeInterval(item.rate)
        }
        for item in pending {
            total += item.targetDuration > 0 ? item.targetDuration : item.duration
        }
        return total
    }

    private func catchupMultiplier() -> TimeInterval {
        guard latencyMode != "complete" else { return 1 }
        let depth = backlog()
        if latencyMode == "low_latency" {
            if depth > 8 { return 1.3 }
            if depth > 4 { return 1.15 }
            return 1
        }
        if depth > 12 { return TimeInterval(rateFaster) }
        if depth > backlogFaster { return TimeInterval(rateFast) }
        return 1
    }

    private func adjustForBacklog() {
        guard latencyMode != "complete" else { return }
        let skipThreshold = latencyMode == "low_latency" ? 8.0 : backlogSkip
        while backlog() > skipThreshold, let index = pending.firstIndex(where: { $0.token != 0 }) {
            let dropped = pending.remove(at: index)
            emit("cancelled", token: dropped.token)
            reportDepth()
        }
    }
}

private final class WavWriter {
    private let handle: FileHandle
    private var dataSize: UInt32 = 0

    init(url: URL) throws {
        FileManager.default.createFile(atPath: url.path, contents: nil)
        handle = try FileHandle(forWritingTo: url)
        try handle.write(contentsOf: WavWriter.header(dataSize: 0))
    }

    func write(_ buffer: AVAudioPCMBuffer) throws {
        guard let samples = buffer.int16ChannelData else { return }
        let size = Int(buffer.frameLength) * MemoryLayout<Int16>.size
        let data = Data(bytes: samples[0], count: size)
        try handle.write(contentsOf: data)
        dataSize += UInt32(size)
    }

    func finish() throws {
        try handle.seek(toOffset: 0)
        try handle.write(contentsOf: WavWriter.header(dataSize: dataSize))
        try handle.close()
    }

    private static func header(dataSize: UInt32) -> Data {
        var data = Data()
        data.append(contentsOf: [82, 73, 70, 70])
        append(36 + dataSize, to: &data)
        data.append(contentsOf: [87, 65, 86, 69, 102, 109, 116, 32])
        append(16, to: &data)
        append16(1, to: &data)
        append16(1, to: &data)
        append(24_000, to: &data)
        append(48_000, to: &data)
        append16(2, to: &data)
        append16(16, to: &data)
        data.append(contentsOf: [100, 97, 116, 97])
        append(dataSize, to: &data)
        return data
    }

    private static func append(_ value: UInt32, to data: inout Data) {
        var littleEndian = value.littleEndian
        withUnsafeBytes(of: &littleEndian) { data.append(contentsOf: $0) }
    }

    private static func append16(_ value: UInt16, to data: inout Data) {
        var littleEndian = value.littleEndian
        withUnsafeBytes(of: &littleEndian) { data.append(contentsOf: $0) }
    }
}

final class Synthesizer {
    private let synthesizer = AVSpeechSynthesizer()
    private let player: Player
    private var pending: [Command] = []
    private var active: Command?
    private var generation: UInt64 = 0

    init(player: Player) {
        self.player = player
    }

    func handle(_ command: Command) {
        DispatchQueue.main.async {
            switch command.cmd {
            case "speak":
                self.pending.append(command)
                self.updateDepth()
                self.pump()
            case "flush":
                self.generation += 1
                var cancelled = self.pending.compactMap { $0.token }
                self.pending.removeAll()
                if let token = self.active?.token { cancelled.append(token) }
                self.active = nil
                self.synthesizer.stopSpeaking(at: .immediate)
                for token in cancelled { self.player.emit("cancelled", token: token) }
                self.updateDepth()
            case "stop":
                self.synthesizer.stopSpeaking(at: .immediate)
            default:
                break
            }
        }
    }

    private func updateDepth() {
        player.setSynthesisDepth(pending.count + (active == nil ? 0 : 1))
    }

    private func pump() {
        guard active == nil, !pending.isEmpty else { return }
        let command = pending.removeFirst()
        guard let token = command.token, let path = command.path, let text = command.text else {
            pump()
            return
        }
        active = command
        updateDepth()
        let requestGeneration = generation
        let utterance = AVSpeechUtterance(string: text)
        if let voiceName = command.voice {
            utterance.voice = AVSpeechSynthesisVoice.speechVoices().first {
                $0.name == voiceName || $0.identifier == voiceName
            } ?? AVSpeechSynthesisVoice(language: voiceName)
        }
        if let wordsPerMinute = command.rate {
            utterance.rate = min(max(Float(wordsPerMinute) / 350, AVSpeechUtteranceMinimumSpeechRate), AVSpeechUtteranceMaximumSpeechRate)
        }
        let url = URL(fileURLWithPath: path)
        guard let format = AVAudioFormat(commonFormat: .pcmFormatInt16, sampleRate: 24_000, channels: 1, interleaved: true) else {
            finish(command: command, requestGeneration: requestGeneration, path: nil, error: "unsupported output format")
            return
        }
        var outputFile: WavWriter?
        var converter: AVAudioConverter?
        var failed = false
        var wroteAudio = false
        synthesizer.write(utterance) { audioBuffer in
            guard let buffer = audioBuffer as? AVAudioPCMBuffer else {
                failed = true
                return
            }
            if buffer.frameLength == 0 {
                do {
                    try outputFile?.finish()
                } catch {
                    failed = true
                }
                outputFile = nil
                if !wroteAudio { failed = true }
                DispatchQueue.main.async {
                    self.finish(command: command, requestGeneration: requestGeneration, path: failed ? nil : path, error: failed ? "speech synthesis failed" : nil)
                }
                return
            }
            guard !failed else { return }
            do {
                if outputFile == nil {
                    outputFile = try WavWriter(url: url)
                }
                if converter == nil {
                    converter = AVAudioConverter(from: buffer.format, to: format)
                }
                guard let converter else {
                    throw NSError(domain: "SimulMTTTS", code: 1, userInfo: [NSLocalizedDescriptionKey: "audio converter unavailable"])
                }
                let ratio = 24_000 / buffer.format.sampleRate
                let capacity = AVAudioFrameCount(Double(buffer.frameLength) * ratio) + 1024
                guard let converted = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: capacity) else {
                    throw NSError(domain: "SimulMTTTS", code: 2, userInfo: [NSLocalizedDescriptionKey: "audio buffer allocation failed"])
                }
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
                if let conversionError { throw conversionError }
                if converted.frameLength > 0 {
                    try outputFile?.write(converted)
                    wroteAudio = true
                }
            } catch {
                failed = true
                DispatchQueue.main.async {
                    self.player.emit("error", token: token, message: error.localizedDescription)
                }
            }
        }
    }

    private func finish(command: Command, requestGeneration: UInt64, path: String?, error: String?) {
        guard requestGeneration == generation, active?.token == command.token else { return }
        active = nil
        updateDepth()
        if let error {
            player.emit("error", token: command.token, message: error)
        } else if let path, let token = command.token {
            player.enqueue(token: token, path: path, sourceDurationMs: command.source_duration_ms)
        }
        pump()
    }
}

let player = Player()
let synthesizer = Synthesizer(player: player)
DispatchQueue.global(qos: .userInitiated).async {
    while let line = readLine(strippingNewline: true) {
        guard let data = line.data(using: .utf8) else { continue }
        do {
            let command = try JSONDecoder().decode(Command.self, from: data)
            if command.cmd == "speak" || command.cmd == "flush" || command.cmd == "stop" {
                synthesizer.handle(command)
            }
            player.handle(command)
        } catch {
            DispatchQueue.main.async {
                player.emit("error", message: "invalid command: \(error.localizedDescription)")
            }
        }
    }
    DispatchQueue.main.async {
        synthesizer.handle(Command(cmd: "stop", token: nil, path: nil, text: nil, voice: nil, rate: nil, source_duration_ms: nil, latency_mode: nil))
        player.handle(Command(cmd: "stop", token: nil, path: nil, text: nil, voice: nil, rate: nil, source_duration_ms: nil, latency_mode: nil))
    }
}

FileHandle.standardError.write("simulmt-tts-host ready pid=\(getpid())\n".data(using: .utf8)!)
RunLoop.main.run()
