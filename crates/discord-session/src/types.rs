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
    /// Resolve a call target only when required routing information is present.
    /// Partial Gateway channel objects can be completed with `Api::channel`.
    pub fn target(&self) -> Option<CallTarget> {
        if self.id.0 == 0 {
            return None;
        }
        let (guild, peer) = match self.kind {
            1 if self.guild_id.is_none() && self.recipients.len() == 1 => {
                let peer = self.recipients[0].id;
                if peer.0 == 0 {
                    return None;
                }
                (None, Some(peer))
            }
            2 => (Some(self.guild_id.filter(|g| g.0 != 0)?), None),
            3 if self.guild_id.is_none() => (None, None),
            _ => return None,
        };
        Some(CallTarget {
            channel: self.id,
            guild,
            peer,
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
    /// A destination changed; refresh it with `Api::channel` if displayed.
    ChannelChanged {
        channel: Id,
    },
    ChannelDeleted {
        channel: Id,
    },
    Call {
        channel: Id,
        /// None means this field was omitted from a partial call update.
        ringing: Option<Vec<Id>>,
        /// None preserves the previous membership; Some(empty) clears it.
        participants: Option<Vec<Participant>>,
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn call_targets_require_complete_routing_information() {
        let mut channel = Channel {
            id: Id(10),
            kind: 2,
            guild_id: None,
            name: None,
            recipients: vec![],
        };
        assert!(channel.target().is_none());
        channel.guild_id = Some(Id(20));
        assert_eq!(channel.target().unwrap().guild, Some(Id(20)));
        channel.kind = 1;
        assert!(channel.target().is_none());
        channel.guild_id = None;
        assert!(channel.target().is_none());
        channel.recipients.push(User {
            id: Id(2),
            username: "peer".into(),
            global_name: None,
            bot: false,
        });
        assert_eq!(channel.target().unwrap().peer, Some(Id(2)));
        channel.recipients.push(channel.recipients[0].clone());
        assert!(channel.target().is_none());
        channel.kind = 3;
        assert_eq!(channel.target().unwrap().peer, None);
        channel.kind = 0;
        assert!(channel.target().is_none());
    }
}
