# Live validation procedure

**Status: not executed.** Local fixture results are recorded separately in
[implementation-status.md](implementation-status.md). Do not mark a row passed
from a successful connection, a packet counter, or another row's result.

## Setup

Use a Linux workstation/Hub with the native dependencies from call-media's
README, selected microphone/speaker and camera, and a second consenting
participant using an official Discord client. For HDMI checks, connect the
capture card and its associated audio device. Keep the Hub UI work separate:
these checks establish the library's behavior through its public APIs.

1. Record both repository revisions, OS, device models, driver and GStreamer
   versions, official client version, and whether the destination is a DM,
   group DM or guild voice channel.
2. Configure `DISCORD_TOKEN` locally through your own credential store/environment.
   Never paste it into chat, issue reports, shell command text or logs.
3. Run `cargo run -p discord-core --example devices --features devices` to list
   microphone, speaker and camera IDs. Enumeration does not start capture.
4. Configure `DISCORD_CHANNEL`; set `DISCORD_GUILD` for server voice, or
   `DISCORD_PEER` for a one-to-one DM. Set selected device IDs in `AUDIO_INPUT`,
   `AUDIO_OUTPUT`, and `CAMERA_DEVICE`. Explicit selections avoid guessing hardware.
5. Run `cargo run -p discord-core --example call --features devices` to join an
   existing call without ringing. Ctrl-C leaves. The example plays audio but
   discards decoded incoming video; visual assessment needs the application's
   renderer consuming `MediaIo.remote_video`. Use no recording unless requested.
6. For HDMI, set `SHARE_VIDEO_DEVICE` to the integer suffix of `/dev/videoN` and
   choose one of `SHARE_AUDIO_PULSE` or `SHARE_AUDIO_ALSA`. These are native device
   names, not the CPAL microphone identifiers printed by the devices example.

## Required checks

| Check | Procedure / expected evidence | Result |
|---|---|---|
| Discovery/authentication | READY and destination lookup identify the intended account/channel without secrets in output | Not run |
| DM audio | Both parties hear speech, no loopback, expected mute/deafen behavior | Not run |
| Group/guild audio | Multiple peers join/leave; identities and speaking events match actual speakers | Not run |
| Ring/answer/decline | Explicit `Handle::call` rings once; answer joins without another ring; decline stops local ringing | Not run |
| Camera transmit | Official client displays live camera movement; disable/enable changes publication correctly | Not run |
| Camera receive | Render RGBA frames; participant identity, motion, color and aspect ratio are correct | Not run |
| Watch Go Live | `Handle::watch` receives correct participant video and stream audio; stop releases work | Not run |
| HDMI sharing | Official client sees capture-card motion and hears its chosen audio; compare lip/clap synchronization | Not run |
| Simultaneous media | Camera, microphone, remote audio/video and sharing remain usable together | Not run |
| Cancellation | Stop while allocating a stream, then retry after departure; no stale stream appears | Not run |
| Network disruption | Local media stops on Gateway/server withdrawal; explicit rejoin succeeds without duplicate rings | Not run |
| Device loss/switch | Unplug or select another device; observe errors/recovery and verify the selected replacement | Not run |
| Acoustic processing | Speakerphone echo/noise at realistic volumes, tested against the remote participant's experience | Not run |
| Cleanup | Leave, shutdown and restart; camera/card can be reopened by another application; no background media continues | Not run |

The headless example does not implement a complete interactive call UI. Exercise
ringing, watching and control changes through the documented `Handle` methods in
the host integration. If a failure occurs, preserve the repository revision,
scenario, sanitized error and relevant device/plugin information. Do not attach
credential-bearing Gateway payloads or raw microphone/camera content by default.

Fix any protocol/device mismatch in the owning crate, reproduce it locally where
possible, then rerun the affected live rows. Completing this table is necessary
before claiming live Discord and Hub support.
