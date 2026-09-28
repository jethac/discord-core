# Dependency routing for Git consumers

Source: crates.io openmls_rust_crypto 0.5.1, upstream commit
`47dbedecad0c1fd8eb5368d582250ebfcc1e1ce6`. Original MIT license retained.

Only Cargo.toml changes: route hpke-rs through the adjacent vendored security
backport. Rust sources are unchanged. This ensures our davey dependency uses
the patched HPKE code when discord-core is a Git dependency, without asking the
application to copy workspace patches. Cargo.toml.orig retains the original.
