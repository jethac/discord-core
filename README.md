# discord-core

Headless Rust libraries for Discord calling and voice/video participation.
Applications own their UI, credentials, hardware selection and rendering.
Public OSS under **MIT OR Apache-2.0**.

## Status

Initial implementation, unstable API, not published to crates.io. **Live Discord
interoperability and physical Hub behavior are unverified.** Local simulated
transport checks are useful evidence, but do not establish working Discord
calls. This repository is under active development.

| Crate | Responsibility |
|---|---|
| `discord-core` | Client/handle API, state/events, call and stream coordination |
| `discord-session` | REST, main Gateway, destinations, call events, ringing, joining/leaving and reconnect |
| `discord-media` | Voice Gateway/UDP, DAVE, Opus, participant mixing, H.264 camera and Go Live send/receive |

The transport accepts caller-owned bounded media queues. Optional `devices` on
`discord-core` enables the companion [call-media](https://github.com/jethac/call-media)
adapters, pinned to a Git commit. There is no Slint, browser or desktop-shell
dependency. Meet protocol support is future work.

## Integration

1. Supply `Secret` explicitly to `Client::connect`. Keep `Client` alive across UI
   navigation. Wait for `Event::Signaling(session::Event::Ready { .. })` before
   joining, and continually drain events.
2. Select a `CallTarget` from REST/channel information. For one-to-one DMs set
   `peer` to the other user. Use `join`/`answer` for existing calls; `call` rings
   once for a DM/group call. Ring requests are never automatically retried.
3. Create `media_channels` or supply `MediaIo`. PCM is mono f32, 48 kHz, 960 samples
   per frame. Feed media only when ready; never block device callbacks on queues.
4. Camera frames are complete H.264 access units, 640x480 at 15 fps on a 90 kHz
   timestamp clock. Tag frames with `State.camera_generation`; zero means off.
   Incoming `RemoteFrame` pixels are borrowed RGBA: copy into a bounded rendering
   queue before returning from the decoder callback.
5. `Handle::share` takes `media::screen::Video` for screen/HDMI sharing.
   `Handle::watch` takes a participant ID and video sink; stream audio is mixed
   into call playback. Share and watch can run alongside camera and call audio.
6. Apply `set_controls` for mute, deafen and camera. Server mute/deafen restrictions
   are tracked independently and applied locally. `leave` cancels local media
   before signaling departure; `close` shuts down the session.

Default builds open no devices. The application may manage all capture/codec
work itself. Optional device adapters need the Linux native libraries described
in [call-media's README](https://github.com/jethac/call-media#native-dependencies).

### Headless example

```sh
cargo run -p discord-core --example call --features devices
```

Set `DISCORD_TOKEN` through your local credential environment, `DISCORD_CHANNEL`,
and optionally `DISCORD_GUILD`/`DISCORD_PEER`. `AUDIO_INPUT`, `AUDIO_OUTPUT`, and
`CAMERA_DEVICE` accept the IDs returned by call-media device enumeration. Camera
capture is enabled only when `CAMERA_DEVICE` is set. The example joins without
ringing, plays received audio, decodes/discards incoming video, and leaves on
Ctrl-C. It does not provide login or a renderer. Do not paste credentials into
issues or logs.

## Limits and remaining work

- Reconnect restores main Gateway state but does not silently rejoin/rering a
  call. A Gateway disconnect or voice-server migration stops local media and
  requires explicit rejoin.
- One active call, one outgoing share and one watched stream; media membership
  and speaker mixing are bounded to 64 participants.
- Camera negotiation is currently H.264 640x480/15 fps; sharing supports selected
  resolutions through 1080p. Unsupported remote codecs are not decoded.
- HDMI video uses an explicit Linux V4L2 node. HDMI audio must be supplied
  separately; automatic audio-device pairing is not implemented.
- Device and protocol coverage on macOS/Windows has not been established.
- Call lifecycle, device switching, stream replacement and simultaneous
  camera/share behavior still need live end-to-end validation.

See [implementation status](docs/implementation-status.md) for evidence and
[THIRD_PARTY.md](THIRD_PARTY.md) for attribution. Transport code is adapted from
Serein; its upstream documentation also marks live interoperability unverified.
The vendored DAVE/OpenMLS/HPKE dependency chain uses explicit paths so Git
consumers inherit our patches without copying root Cargo patches.

## Development

```sh
cargo fmt --all -- --check
cargo check --workspace --all-features --all-targets --locked
cargo clippy --workspace --all-features --all-targets --locked -- -D warnings
cargo doc --workspace --all-features --no-deps --locked
```

All features require native development packages. Default library builds use
bundled software codecs and require a C/C++ toolchain.

## License

Our crates are licensed under either [Apache-2.0](LICENSE-APACHE) or
[MIT](LICENSE-MIT), at your option. Contributions are dual licensed on the same
terms unless stated otherwise. Vendored dependencies retain their original
licenses, including MPL-2.0 for hpke-rs; see THIRD_PARTY.md.
