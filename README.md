# discord-core

Headless Rust libraries for Discord calling and voice/video participation.
Designed for applications that own their UI and hardware integration.

**Status: initial workspace scaffold. No Discord connection or calling is implemented.**
The packages are not published to crates.io and do not yet define a stable consumer API.

## Workspace

| Crate | Planned responsibility |
|---|---|
| `discord-core` | Application-facing client facade, commands, and events |
| `discord-session` | Authenticated REST and main Gateway, destinations, incoming calls, ringing, answering, joining, leaving, and connection lifecycle |
| `discord-media` | Voice Gateway, UDP, DAVE encryption, Opus, participant audio, camera video, and incoming streams |

The session and media crates live together because call signaling and media
negotiation need coordinated changes. Public APIs will use ordinary Rust types.
Credentials will be supplied by the consumer; login presentation and credential
storage policy belong to the application.

[call-media](https://github.com/jethac/call-media) is the companion repository for
optional audio/video device and codec adapters. The transport should also accept
caller-provided media. No dependency between these workspaces is committed yet.

## Implementation order

1. Audit candidate protocol implementations and their dependency patches; establish a small headless API.
2. Demonstrate join/leave and two-way encrypted audio in a headless example.
3. Establish DM and group call lifecycle, participant changes, mute/deafen, and disconnect behavior.
4. Establish incoming video and outgoing camera interoperability, then sharing.
5. Document validated behavior and publish usable crates.

See [the source assessment](docs/source-assessment.md) for candidate code and
limitations. Serein is a source candidate, not a validated backend: its detailed
voice documentation explicitly marks live Discord interoperability untested.
No upstream implementation has been copied into this scaffold.

## Development

```sh
cargo check --workspace --all-targets --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo doc --workspace --no-deps --locked
```

## License

Licensed under either the Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
or the MIT license ([LICENSE-MIT](LICENSE-MIT)), at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
