# Source and dependency attribution

The `discord-media` transport, crypto coordination, RTP, jitter, mixer and video
implementation is adapted from Serein at
`1ecf8d1695728e42ec4f47174fa1dcbd160f859e` (MIT OR Apache-2.0).
Copyright (c) 2026 Serein contributors. Original license texts are in `LICENSES/`.
Desktop dependencies are replaced by the `call-media` adapter and narrow session
types. No live Discord compatibility is implied by this extraction.

`vendor/davey` retains its upstream MIT license. `vendor/hpke-rs` retains MPL-2.0;
its source and modifications are distributed here under that license. They are
not relicensed as our dual-licensed code. Their `SEREIN-PATCH.md` files record the
exact upstream revisions, manifest/native dependency changes, and security
backport provenance. The dependency chain uses explicit relative paths, so Git consumers inherit
these versions without root Cargo patches. `vendor/openmls_rust_crypto` retains
its MIT license and unchanged Rust sources; its manifest routes HPKE to the
adjacent backport. See its `PATCH.md` for provenance.
