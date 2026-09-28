# Implementation and validation status

This is an implementation checkpoint, not a declaration of live support.

| Requirement | Current implementation | Evidence still needed |
|---|---|---|
| Normal user session | Authenticated REST and Gateway with explicit credentials, bounded events, reconnect/resume | Live authentication and destination discovery |
| DM/group/guild calls | Join, ring, answer, decline, leave, participant events | Live call lifecycle across each destination type |
| Encrypted bidirectional audio | UDP, DAVE, Opus, jitter buffering and mixing | Live peer interoperability and physical acoustic quality |
| Camera send/receive | H.264 packetization, authenticated transport, reassembly and decode | Two-way live camera call with official clients |
| Watch streams | Stream allocation, encrypted video/audio receive | Live Go Live with official client |
| Share video/audio | Stream allocation, H.264 send and stereo audio path | Live sharing, congestion, viewer joining/leaving |
| HDMI | V4L2 sharing source, explicit ALSA/PulseAudio stereo capture | Actual capture card, audio pairing and sync |
| Device adapters | Optional call-media capture, playback, AEC/noise and codecs | Hub device switching, disconnects, release, echo and latency |
| Host integration | Headless API, bounded queues, ownership and runnable example | Hub rendering and UI integration by application agent |
| Delivery | Public repositories, dual licenses and retained dependency notices | Stable API and eventual release after validation |

## Recorded local checks

The latest local all-feature run recorded:

- `discord-core`: 3 passing checks (queue bounds, worker cancellation and stale updates).
- `discord-media`: 43 passing checks, 2 ignored performance benchmarks.
- `discord-session`: 11 passing checks, including partial updates and stream deletion fencing.
- Companion `call-media`: 30 passing checks, including SPS coded/cropped bounds and synthetic codec roundtrips.

The transport checks include a loopback WebSocket/UDP voice service, DAVE
transitions, Opus, and encrypted H.264 stream send/receive. They did not contact
Discord or use Hub devices. Native hardware-dependent checks may return early
when a backend is unavailable; a passing suite is not proof of hardware coverage.
Both repositories now run their all-feature regression suites in CI.

There is still no full facade-to-device live call exercise. The standalone share
audio adapter needs real-source and encryption-transition exercises, and SPS
preflight needs broader supported-profile/bitstream coverage beyond its boundary
cases. Those requirements remain open.

## Current lifecycle policy

Cancelled stream worker updates are matched by call and stream generation.
Stream keys remain reserved until deletion acknowledgment, with a deadline to
recover from missing allocation/deletion events. Partial call updates preserve
omitted membership/ringing fields as `None` instead of reporting empty lists.
Camera generations invalidate buffered frames after camera toggles. Gateway
disconnect and server withdrawal stop local media; an explicit join is needed
once ready. This favors a visible recoverable error over unintended repeated
ringing. Automatic media reconnection remains future implementation work.

## Decoder preflight and stream readiness

All incoming H.264 decoder paths now preflight sequence parameter declarations
for supported profiles/chroma/depth, coded surface dimensions and cropped output
bounds before passing bytes to a native decoder. The preflight is not a complete
bitstream verifier; broader bitstream and live codec coverage remain outstanding.

Stream allocation emits `Event::StreamState` with `Phase::ConnectingMedia`. `StreamStarted` is emitted on encrypted media readiness,
not allocation. Applications should use subsequent stream state events when
showing encryption transitions or waiting for a peer.

## Destination discovery

The REST API exposes `channel`, `user` and explicit `create_dm` alongside private
channels and guild channel enumeration. Channel routing requires a known guild
for server voice and one known peer for one-to-one DMs. Destination-change events
allow consumers to refresh cached display data. Local HTTP fixtures exercise the
request path, explicit DM recipient body, response IDs and invalid routing data.
Live normal-user endpoint behavior remains unverified.

Endpoint schemas are based on Discord's [User resource](https://docs.discord.com/developers/resources/user)
and [Channel resource](https://docs.discord.com/developers/resources/channel).
