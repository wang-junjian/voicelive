#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
mkdir -p target/native runtime/cache

cargo build --workspace --locked
swiftc -O adapters/audio/macos/audio_host.swift -o target/native/simulmt-audio-host \
  -framework Foundation -framework AppKit -framework CoreAudio -framework AudioToolbox
swiftc -O adapters/sinks/native-player/tts_host.swift -o target/native/simulmt-tts-host \
  -framework Foundation -framework AVFoundation
swiftc -O apps/desktop/src/main.swift -o target/native/simulmt-desktop \
  -framework Cocoa -framework WebKit

echo "Built SimulMT core, audio host, TTS host, and desktop console."
