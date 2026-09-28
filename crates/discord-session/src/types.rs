use crate::{Error, Id, VoiceConnection};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct User {
    pub id: Id,
    pub username: String,
    #[serde(default)]
    pub global_name: Option<String>,
    #[serde(default)]
    pub bot: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Channel {
    pub id: Id,
    #[serde(rename = "type")]
    pub kind: u8,
    #[serde(default)]
    pub guild_id: Option<Id>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub recipients: Vec<User>,
}
impl Channel {
    pub fn callable(&self) -> bool {
        matches!(self.kind, 1..=3)
    }
    pub fn target(&self) -> Option<CallTarget> {
        self.callable().then(|| CallTarget {
            channel: self.id,
            guild: self.guild_id,
            peer: if self.kind == 1 {
                self.recipients.first().map(|u| u.id)
            } else {
                None
            },
        })
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Guild {
    pub id: Id,
    pub name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallTarget {
    pub channel: Id,
    pub guild: Option<Id>,
    /// Restrict DAVE membership to this peer for a one-to-one DM.
    pub peer: Option<Id>,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Controls {
    pub muted: bool,
    pub deafened: bool,
    pub camera: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Participant {
    pub user_id: Id,
    pub channel_id: Option<Id>,
    #[serde(default)]
    pub guild_id: Option<Id>,
    #[serde(default)]
    pub self_mute: bool,
    #[serde(default)]
    pub self_deaf: bool,
    #[serde(default)]
    pub self_video: bool,
    #[serde(default)]
    pub self_stream: bool,
    #[serde(default)]
    pub mute: bool,
    #[serde(default)]
    pub deaf: bool,
}

#[derive(Debug)]
pub enum Event {
    Ready {
        user: User,
        channels: Vec<Channel>,
        guilds: Vec<Guild>,
    },
    Resumed,
    Disconnected,
    Call {
        channel: Id,
        ringing: Vec<Id>,
        participants: Vec<Participant>,
    },
    CallEnded {
        channel: Id,
    },
    Participant(Participant),
    VoiceReady(VoiceConnection),
    /// The current voice server was withdrawn; stop using its transport.
    VoiceUnavailable {
        request: u64,
    },
    Left {
        request: u64,
    },
    StreamReady {
        key: String,
        connection: VoiceConnection,
    },
    StreamEnded {
        key: String,
    },
    Error(Error),
}
