# Rust libraries for Discord calling

Scope: reusable Rust libraries for normal-account Discord calls, server voice participation, camera video, and incoming streams. The Hub application and UI belong to another agent. Google Meet is future work.

## Findings, 2026-09-28

Inspected Serein at `1ecf8d1695728e42ec4f47174fa1dcbd160f859e`:
https://github.com/ViceVerse-cz/Serein/tree/1ecf8d1695728e42ec4f47174fa1dcbd160f859e

Its source contains real session, signaling, audio, encryption and video implementations. However, `docs/voice.md` explicitly says live Discord interoperability and physical microphone/speaker behavior have not been tested. Camera send and video receive modules also mark live interoperability unverified. Treat it as a candidate implementation requiring validation, not a working SDK established by its README feature list.

### Existing boundaries

- `discord-api`: HTTP API, including DM ring and stop-ringing; depends on `client-core`, `model`, and `discord-protocol`.
- `discord-gateway`: connection and voice commands; `run_with_voice` consumes commands and emits client-core events.
- `client-core`, `model`, `session-cache`, `discord-protocol`: application state and protocol types shared with the desktop app. They do not directly depend on egui in the inspected manifests, but include functionality beyond calling.
- `discord-voice`: voice WebSocket, UDP, Opus, DAVE, mixing, jitter handling, camera/stream transport, capture and playback.
- The voice transport accepts bounded queues of 960-sample mono f32 frames at 48 kHz (20 ms), outgoing encoded camera frames, a watch channel of controls, and a remote video callback. The transport entry point does not open audio devices.
- Remote decoded video is delivered as borrowed RGBA with user ID and dimensions. Consumers must copy or otherwise take ownership before the callback returns; queues must remain bounded.
- The outgoing camera frame type contains generation, timestamp, and encoded bytes. Preserve the actual codec and timestamp conventions when adapting it; this is not a raw camera-pixel input.

### Extraction concerns

`discord-voice` directly depends on `platform`. Its use of that crate is in the incoming H.264 hardware decoder. The platform manifest brings GTK4, WebKit6, winit, keyring, notifications, hotkeys and other desktop facilities. Move the decoder into a media-specific adapter or inject it through an interface before offering the voice crate as a standalone library.

The workspace patches `davey` and `hpke-rs` to vendored versions. A downstream Git dependency does not automatically inherit a dependency workspace's root patches. Audit and deliberately preserve or eliminate those changes; merely pointing Cargo at the voice package is insufficient.

The inspected workspace declares Rust 1.98 and MIT OR Apache-2.0. Any extraction must retain applicable source attribution and licenses, including those of vendored components.

## Proposed crate responsibilities

These are design proposals, not implemented packages or reserved crates.io names.

| Crate | Responsibility |
|---|---|
| `discord-session` | Authenticated REST and main Gateway, discovery of callable destinations, incoming call state, ring/answer/join/leave, connection lifecycle and typed errors |
| `discord-media` | Voice Gateway and UDP, DAVE state transitions, Opus, audio playout, camera/video stream signaling and transport; accepts media inputs and emits outputs |
| `call-media` | Optional Linux device/codec adapter: selected audio devices, camera or HDMI frames, encoding/decoding, echo cancellation and noise suppression |

Expose a small facade if it simplifies consumption. Keep UI framework types out of public APIs. Model direct messages, group DMs and server channels explicitly. Distinguish requested connection, transport readiness, encryption readiness and usable media. A reconnect must not duplicate a ringing action. Cancellation and hangup must release media promptly.

The application supplies credentials through an explicit API and owns login presentation/storage policy. Media device ownership must be agreed with the application so previews and calls do not race to open the same camera. Support caller-provided media as well as an optional device adapter.

## Reuse candidates

- Serein: candidate source for normal-account session and call coordination, media transport and video handling; live behavior remains unverified. https://github.com/ViceVerse-cz/Serein
- Songbird: established Rust voice library with standalone driver and audio receive support. Investigate as an audio transport alternative; its documented bot/guild integration does not establish the required normal-account DM or video behavior. https://github.com/serenity-rs/songbird
- davey: Rust DAVE implementation using OpenMLS. It handles encryption mechanics; the caller still owns signaling and transitions. https://github.com/Snazzah/davey
- Serein's current media dependencies provide a concrete candidate stack: opus2, cpal, sonora, nnnoiseless, openh264, and Linux GStreamer bindings. These are candidates pending independent API, version, deployment and runtime validation.

## Implementation sequence and evidence needed

1. Extract or adapt the narrow session/media boundary, remove desktop platform dependencies, and audit upstream patches.
2. Provide a headless example taking externally supplied credentials and an explicit call target. Prove join/leave and two-way encrypted audio with controlled participants.
3. Exercise DM ringing/answering and group membership transitions, mute/deafen, device changes and connection loss.
4. Establish incoming video and outgoing camera interoperability, then HDMI sharing as a separate capability.
5. Publish the consumer API, dependency/system-library requirements, and precise validated feature matrix for the Hub agent.

No builds, tests, live Discord sessions, or physical device checks were performed for this assessment. No credentials were requested or accessed. This document records source inspection and proposed boundaries only.
