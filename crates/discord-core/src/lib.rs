//! Headless Rust client facade for Discord calling (initial scaffold).
//!
//! No runtime implementation is provided yet.

/// Discord voice and video transport boundary.
pub use discord_media as media;
/// Discord session and call signaling boundary.
pub use discord_session as session;
