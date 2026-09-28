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

Before the latest lifecycle changes, the local media suite recorded 43 passing
checks and two ignored performance benchmarks; session checks recorded seven
passes; call-media recorded 28 passes. These included a loopback WebSocket/UDP
voice service, DAVE transitions, Opus, H.264 stream send/receive, synthetic codec
roundtrips and audio processing. They did not contact Discord or use Hub devices.

The subsequent stream-generation, server-restriction and server-withdrawal
changes have build/static analysis evidence only at this checkpoint. Regression
coverage and live exercises are still needed. Do not treat the older suite
results as verification of those changes.

## Current lifecycle policy

Cancelled stream worker updates are matched by call and stream generation.
Stream keys remain reserved until deletion acknowledgment, with a deadline to
recover from missing allocation/deletion events. Partial call updates preserve
omitted membership/ringing fields as `None` instead of reporting empty lists.
Camera generations invalidate buffered frames after camera toggles. Gateway
disconnect and server withdrawal stop local media; an explicit join is needed
once ready. This favors a visible recoverable error over unintended repeated
ringing. Automatic media reconnection remains future implementation work.
