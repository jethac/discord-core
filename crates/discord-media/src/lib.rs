//! Discord voice/video transport, with caller-owned media inputs and outputs.
//! No device is opened by this crate. All queues must be bounded by the caller.
mod activity;
pub mod camera_video;
mod capture;
mod crypto;
#[allow(dead_code)]
mod diagnostics;
mod jitter;
mod mixer;
mod stream_playout;
mod timer;
mod transport;
mod video;
mod video_receive;
mod video_sps;
pub use call_media::Frame;
pub use call_media::screen;
pub use crypto::Identity;
pub use transport::{run, run_stream, run_with_identity, watch_stream};
pub use video_receive::{RemoteFrame, VideoSink};
#[derive(Clone, Copy)]
pub struct Controls {
    pub muted: bool,
    /// Local indicator threshold; independent of received participants.
    pub activity_threshold_db: i16,
    /// Zero means off; a new value invalidates frames from the previous camera instance.
    pub camera: u64,
    pub deafened: bool,
    /// Session-only playback percentages (0–200); zero user IDs are unused.
    pub user_volumes: [(u64, u16); 64],
    /// Watched stream playback percentage, independently muted with zero.
    pub stream_volume: u16,
}
impl Default for Controls {
    fn default() -> Self {
        Self {
            muted: false,
            activity_threshold_db: -45,
            camera: 0,
            deafened: false,
            user_volumes: [(0, 100); 64],
            stream_volume: 100,
        }
    }
}
pub enum Status {
    Connecting,
    Discovering,
    TransportReady,
    CameraAvailable(bool),
    Securing,
    WaitingForPeer,
    Ready {
        privacy_code: String,
    },
    RemoteAudio,
    /// Latest active user IDs, zero-padded to the 64-participant limit.
    Speaking(Box<[u64; 64]>),
}

#[cfg(test)]
mod test_mls;
