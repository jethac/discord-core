//! Main Gateway and bounded call signaling. Call writes are never replayed on reconnect.
use crate::{
    Api, CallTarget, Channel, Controls, Error, Event, Guild, Id, Participant, Secret, User,
    VoiceConnection,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    connect_async_tls_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

pub(crate) fn validate_url(raw: &str) -> Result<String, Error> {
    let mut url = url::Url::parse(raw).map_err(|_| Error::Protocol)?;
    let host = url.host_str().ok_or(Error::Protocol)?;
    if raw.len() > 512
        || url.scheme() != "wss"
        || !(host == "gateway.discord.gg" || host.ends_with(".discord.gg"))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.port().is_some_and(|p| p != 443)
    {
        return Err(Error::Protocol);
    }
    url.set_query(Some("v=10&encoding=json"));
    Ok(url.to_string())
}

#[derive(Debug)]
enum Command {
    Join(CallTarget, Controls),
    Leave,
    Controls(Controls),
    Sync(Id),
    StartStream,
    WatchStream(Id),
    StopStream(String),
    Shutdown,
}
struct Request {
    command: Command,
    reply: oneshot::Sender<Result<u64, Error>>,
}

/// Cloneable command sender. Acknowledgement means the Gateway write was accepted locally.
/// Transport/encryption readiness arrives separately as events from the media engine.
#[derive(Clone)]
pub struct SessionHandle {
    commands: mpsc::Sender<Request>,
    api: Arc<Api>,
    owner: Id,
}
impl SessionHandle {
    async fn command(&self, command: Command) -> Result<u64, Error> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Request { command, reply })
            .await
            .map_err(|_| Error::Closed)?;
        // A timed out write must not be blindly retried: it may have reached the service.
        timeout(Duration::from_secs(20), receive)
            .await
            .map_err(|_| Error::Ambiguous)?
            .map_err(|_| Error::Closed)?
    }
    pub fn api(&self) -> &Arc<Api> {
        &self.api
    }
    pub fn user_id(&self) -> Id {
        self.owner
    }
    /// Join or answer without ringing. To initiate a DM call, ring once after VoiceReady.
    pub async fn join(&self, target: CallTarget, controls: Controls) -> Result<u64, Error> {
        self.command(Command::Join(target, controls)).await
    }
    pub async fn leave(&self) -> Result<(), Error> {
        self.command(Command::Leave).await.map(|_| ())
    }
    pub async fn set_controls(&self, controls: Controls) -> Result<(), Error> {
        self.command(Command::Controls(controls)).await.map(|_| ())
    }
    pub async fn sync_call(&self, channel: Id) -> Result<(), Error> {
        self.command(Command::Sync(channel)).await.map(|_| ())
    }
    pub async fn start_stream(&self) -> Result<(), Error> {
        self.command(Command::StartStream).await.map(|_| ())
    }
    pub async fn watch_stream(&self, user: Id) -> Result<(), Error> {
        self.command(Command::WatchStream(user)).await.map(|_| ())
    }
    pub async fn stop_stream(&self, key: String) -> Result<(), Error> {
        self.command(Command::StopStream(key)).await.map(|_| ())
    }
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.command(Command::Shutdown).await.map(|_| ())
    }
}

/// Owns the Gateway worker. Dropping the session aborts it and closes its socket.
/// Retain it independently of the application's visible page.
pub struct Session {
    handle: SessionHandle,
    events: mpsc::Receiver<Event>,
    worker: JoinHandle<()>,
}
impl Session {
    /// Authenticate, discover the Gateway, and start its background worker on the current runtime.
    pub async fn connect(secret: Secret) -> Result<Self, Error> {
        let secret = Arc::new(secret);
        let api = Arc::new(Api::new(secret.clone())?);
        let owner = api.current_user().await?;
        let url = api.gateway_url().await?;
        let (commands, receive) = mpsc::channel(32);
        let (send, events) = mpsc::channel(128);
        let owner_id = owner.id;
        let worker = tokio::spawn(async move {
            if let Err(error) = run(
                secret,
                owner,
                url,
                receive,
                send.clone(),
                #[cfg(test)]
                None,
            )
            .await
            {
                let _ = send.try_send(Event::Error(error));
            }
        });
        Ok(Self {
            handle: SessionHandle {
                commands,
                api,
                owner: owner_id,
            },
            events,
            worker,
        })
    }
    pub fn handle(&self) -> SessionHandle {
        self.handle.clone()
    }
    pub async fn next_event(&mut self) -> Option<Event> {
        self.events.recv().await
    }
    pub async fn close(mut self) -> Result<(), Error> {
        let result = self.handle.shutdown().await;
        let _ = timeout(Duration::from_secs(5), &mut self.worker).await;
        result
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

struct Active {
    target: CallTarget,
    request: u64,
    controls: Controls,
    session: Option<Secret>,
    server: Option<(Secret, String)>,
    deadline: Option<Instant>,
    leaving: bool,
}
#[derive(Default)]
struct Stream {
    created: Option<(Id, Id)>,
    server: Option<(Secret, String)>,
    stopping: bool,
    deadline: Option<Instant>,
}
#[derive(Default)]
struct Calls {
    active: Option<Active>,
    streams: HashMap<String, Stream>,
    next: u64,
}
impl Calls {
    fn packet(&mut self, command: Command, owner: Id) -> Result<(Option<Value>, u64, bool), Error> {
        match command {
            Command::Join(target, controls) => {
                if self.active.is_some() {
                    return Err(Error::Busy);
                }
                if target.channel.0 == 0
                    || target.guild.is_some_and(|g| g.0 == 0)
                    || target.peer == Some(owner)
                {
                    return Err(Error::Protocol);
                }
                self.next = self.next.checked_add(1).ok_or(Error::Capacity)?;
                self.active = Some(Active {
                    target,
                    controls,
                    request: self.next,
                    session: None,
                    server: None,
                    deadline: Some(Instant::now() + Duration::from_secs(20)),
                    leaving: false,
                });
                Ok((
                    Some(voice_packet(Some(target.channel), target.guild, controls)),
                    self.next,
                    false,
                ))
            }
            Command::Leave | Command::Shutdown => {
                let shutdown = matches!(command, Command::Shutdown);
                let packet = self.active.as_mut().map(|active| {
                    active.leaving = true;
                    active.deadline = Some(Instant::now() + Duration::from_secs(10));
                    voice_packet(
                        None,
                        active.target.guild,
                        Controls {
                            muted: true,
                            deafened: true,
                            camera: false,
                        },
                    )
                });
                self.streams.clear();
                Ok((packet, self.next, shutdown))
            }
            Command::Controls(controls) => {
                let active = self
                    .active
                    .as_mut()
                    .filter(|a| !a.leaving)
                    .ok_or(Error::NotReady)?;
                active.controls = controls;
                Ok((
                    Some(voice_packet(
                        Some(active.target.channel),
                        active.target.guild,
                        controls,
                    )),
                    active.request,
                    false,
                ))
            }
            Command::Sync(channel) => {
                Ok((Some(json!({"op":13,"d":{"channel_id":channel}})), 0, false))
            }
            Command::StartStream | Command::WatchStream(_) => {
                let active = self
                    .active
                    .as_ref()
                    .filter(|a| !a.leaving && a.session.is_some())
                    .ok_or(Error::NotReady)?;
                let streamer = if let Command::WatchStream(user) = command {
                    user
                } else {
                    owner
                };
                if streamer.0 == 0 {
                    return Err(Error::Protocol);
                }
                let key = stream_key(active.target, streamer);
                if self.streams.contains_key(&key) || self.streams.len() >= 2 {
                    return Err(Error::Busy);
                }
                self.streams.insert(
                    key.clone(),
                    Stream {
                        deadline: Some(Instant::now() + Duration::from_secs(20)),
                        ..Default::default()
                    },
                );
                let packet = if matches!(command, Command::StartStream) {
                    json!({"op":18,"d":{"type":if active.target.guild.is_some(){"guild"}else{"call"},"guild_id":active.target.guild,"channel_id":active.target.channel,"preferred_region":null}})
                } else {
                    json!({"op":20,"d":{"stream_key":key}})
                };
                Ok((Some(packet), active.request, false))
            }
            Command::StopStream(key) => {
                let stream = self.streams.get_mut(&key).ok_or(Error::NotReady)?;
                if stream.stopping {
                    return Err(Error::Busy);
                }
                // Keep the key reserved until STREAM_DELETE acknowledges departure.
                // Allocation events carry no client generation, so removing it now
                // could attach old credentials to a replacement using the same key.
                stream.stopping = true;
                stream.created = None;
                stream.server = None;
                stream.deadline = Some(Instant::now() + Duration::from_secs(10));
                Ok((
                    Some(json!({"op":19,"d":{"stream_key":key}})),
                    self.next,
                    false,
                ))
            }
        }
    }
    fn dispatch(
        &mut self,
        name: &str,
        data: &Value,
        owner: Id,
        events: &mpsc::Sender<Event>,
    ) -> Result<(), Error> {
        match name {
            "VOICE_STATE_UPDATE" => {
                let participant: Participant =
                    serde_json::from_value(data.clone()).map_err(|_| Error::Protocol)?;
                if participant.user_id == owner
                    && let Some(active) = self.active.as_mut()
                    && participant.guild_id == active.target.guild
                {
                    if participant.channel_id == Some(active.target.channel) && !active.leaving {
                        active.session = Some(secret(data, "session_id")?);
                    } else if participant.channel_id.is_none()
                        || participant.channel_id != Some(active.target.channel)
                    {
                        emit(
                            events,
                            Event::Left {
                                request: active.request,
                            },
                        )?;
                        self.active = None;
                        self.streams.clear();
                    }
                }
                emit(events, Event::Participant(participant))?;
                self.voice_ready(owner, events)?;
            }
            "VOICE_SERVER_UPDATE" => {
                if let Some(active) = self.active.as_mut().filter(|a| !a.leaving) {
                    let guild = optional_id(data, "guild_id")?;
                    let channel = optional_id(data, "channel_id")?;
                    if guild == active.target.guild
                        && (guild.is_some() || channel == Some(active.target.channel))
                    {
                        // A null endpoint means allocation/migration is still in progress.
                        if data["endpoint"].is_null() {
                            active.server = None;
                            if active.deadline.is_none() {
                                emit(
                                    events,
                                    Event::VoiceUnavailable {
                                        request: active.request,
                                    },
                                )?;
                                active.deadline = Some(Instant::now() + Duration::from_secs(20));
                            }
                        } else {
                            active.server =
                                Some((secret(data, "token")?, text(data, "endpoint", 256)?));
                            self.voice_ready(owner, events)?;
                        }
                    }
                }
            }
            "CALL_CREATE" | "CALL_UPDATE" => {
                let channel = id(data, "channel_id")?;
                let ringing = data
                    .get("ringing")
                    .map(|v| serde_json::from_value::<Vec<Id>>(v.clone()))
                    .transpose()
                    .map_err(|_| Error::Protocol)?;
                let participants = data
                    .get("voice_states")
                    .map(|v| serde_json::from_value::<Vec<Participant>>(v.clone()))
                    .transpose()
                    .map_err(|_| Error::Protocol)?;
                if ringing.as_ref().is_some_and(|r| r.len() > 64)
                    || participants.as_ref().is_some_and(|p| p.len() > 64)
                {
                    return Err(Error::Capacity);
                }
                emit(
                    events,
                    Event::Call {
                        channel,
                        ringing,
                        participants,
                    },
                )?;
            }
            "CALL_DELETE" => {
                emit(
                    events,
                    Event::CallEnded {
                        channel: id(data, "channel_id")?,
                    },
                )?;
            }
            "STREAM_CREATE" | "STREAM_SERVER_UPDATE" | "STREAM_DELETE" => {
                let key = text(data, "stream_key", 128)?;
                if name == "STREAM_DELETE" {
                    if self.streams.remove(&key).is_some() {
                        emit(events, Event::StreamEnded { key })?;
                    }
                } else if let Some(stream) = self.streams.get_mut(&key).filter(|s| !s.stopping) {
                    if name == "STREAM_CREATE" {
                        stream.created =
                            Some((id(data, "rtc_server_id")?, id(data, "rtc_channel_id")?));
                    } else if !data["endpoint"].is_null() {
                        stream.server =
                            Some((secret(data, "token")?, text(data, "endpoint", 256)?));
                    }
                    if stream.created.is_some()
                        && stream.server.is_some()
                        && let Some(active) = self
                            .active
                            .as_ref()
                            .filter(|a| a.session.is_some() && !a.leaving)
                    {
                        stream.deadline = None;
                        let (guild, channel) = stream.created.take().ok_or(Error::Protocol)?;
                        let (token, endpoint) = stream.server.take().ok_or(Error::Protocol)?;
                        emit(
                            events,
                            Event::StreamReady {
                                key,
                                connection: VoiceConnection {
                                    channel,
                                    guild: Some(guild),
                                    user: owner,
                                    peer: None,
                                    session: Secret::new(
                                        active
                                            .session
                                            .as_ref()
                                            .ok_or(Error::Protocol)?
                                            .expose()
                                            .to_owned(),
                                    )?,
                                    token,
                                    endpoint,
                                    request: active.request,
                                },
                            },
                        )?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn voice_ready(&mut self, owner: Id, events: &mpsc::Sender<Event>) -> Result<(), Error> {
        if let Some(active) = self
            .active
            .as_mut()
            .filter(|a| !a.leaving && a.session.is_some() && a.server.is_some())
        {
            let (token, endpoint) = active.server.take().ok_or(Error::Protocol)?;
            active.deadline = None;
            emit(
                events,
                Event::VoiceReady(VoiceConnection {
                    channel: active.target.channel,
                    guild: active.target.guild,
                    user: owner,
                    peer: active.target.peer,
                    session: Secret::new(
                        active
                            .session
                            .as_ref()
                            .ok_or(Error::Protocol)?
                            .expose()
                            .to_owned(),
                    )?,
                    token,
                    endpoint,
                    request: active.request,
                }),
            )?;
        }
        Ok(())
    }
}
fn voice_packet(channel: Option<Id>, guild: Option<Id>, controls: Controls) -> Value {
    json!({"op":4,"d":{"guild_id":guild,"channel_id":channel,"self_mute":controls.muted||controls.deafened,"self_deaf":controls.deafened,"self_video":controls.camera}})
}
fn stream_key(target: CallTarget, user: Id) -> String {
    target.guild.map_or_else(
        || format!("call:{}:{user}", target.channel),
        |g| format!("guild:{g}:{}:{user}", target.channel),
    )
}
fn emit(events: &mpsc::Sender<Event>, event: Event) -> Result<(), Error> {
    events.try_send(event).map_err(|e| match e {
        mpsc::error::TrySendError::Closed(_) => Error::Closed,
        mpsc::error::TrySendError::Full(_) => Error::Capacity,
    })
}
fn id(v: &Value, key: &str) -> Result<Id, Error> {
    serde_json::from_value(v[key].clone()).map_err(|_| Error::Protocol)
}
fn optional_id(v: &Value, key: &str) -> Result<Option<Id>, Error> {
    if v[key].is_null() {
        Ok(None)
    } else {
        id(v, key).map(Some)
    }
}
fn text(v: &Value, key: &str, max: usize) -> Result<String, Error> {
    let s = v[key].as_str().ok_or(Error::Protocol)?;
    if s.is_empty() || s.len() > max {
        return Err(Error::Protocol);
    }
    Ok(s.to_owned())
}
fn secret(v: &Value, key: &str) -> Result<Secret, Error> {
    Secret::new(text(v, key, 2048)?)
}

async fn run(
    secret: Arc<Secret>,
    owner: User,
    initial_url: String,
    mut commands: mpsc::Receiver<Request>,
    events: mpsc::Sender<Event>,
    #[cfg(test)] test_endpoint: Option<&str>,
) -> Result<(), Error> {
    let mut session: Option<Secret> = None;
    let mut sequence: Option<u64> = None;
    let mut resume_url = initial_url.clone();
    let mut attempt = 0u32;
    let mut calls = Calls::default();
    let mut departure = None;
    loop {
        if events.is_closed() {
            return Ok(());
        }
        if attempt > 0 {
            emit(&events, Event::Disconnected)?;
            if let Some(active) = calls.active.take() {
                departure = Some(active.target.guild);
            }
            calls.streams.clear();
            let until = Instant::now() + Duration::from_secs((1u64 << attempt.min(6)).min(60));
            loop {
                tokio::select! {
                    _=tokio::time::sleep_until(until)=>break,
                    request=commands.recv()=>match request {
                        None=>return Ok(()),
                        Some(request)=>{
                            if matches!(request.command,Command::Shutdown){let _=request.reply.send(Ok(0));return Ok(())}
                            let _=request.reply.send(Err(Error::NotReady));
                        }
                    },
                }
            }
        }
        let config = WebSocketConfig::default()
            .max_message_size(Some(8 * 1024 * 1024))
            .max_frame_size(Some(8 * 1024 * 1024));
        let dial = resume_url.as_str();
        #[cfg(test)]
        let dial = test_endpoint.unwrap_or(dial);
        let connection = timeout(
            Duration::from_secs(15),
            connect_async_tls_with_config(dial, Some(config), false, Some(crate::tls::connector())),
        )
        .await;
        let Ok(Ok((mut ws, _))) = connection else {
            attempt = attempt.saturating_add(1);
            continue;
        };
        let hello = timeout(Duration::from_secs(10), ws.next()).await;
        let Ok(Some(Ok(Message::Text(hello)))) = hello else {
            attempt = attempt.saturating_add(1);
            continue;
        };
        let hello: Value = serde_json::from_str(&hello).map_err(|_| Error::Protocol)?;
        let interval = hello["d"]["heartbeat_interval"]
            .as_u64()
            .filter(|n| (1000..=120000).contains(n))
            .ok_or(Error::Protocol)?;
        if hello["op"] != 10 {
            return Err(Error::Protocol);
        }
        let identify = if let (Some(session), Some(sequence)) = (&session, sequence) {
            json!({"op":6,"d":{"token":secret.expose(),"session_id":session.expose(),"seq":sequence}})
        } else {
            json!({"op":2,"d":{"token":secret.expose(),"compress":false,"properties":{"os":std::env::consts::OS,"browser":"discord-core","device":"discord-core"},"presence":{"status":"online","since":0,"activities":[],"afk":false}}})
        };
        if timeout(
            Duration::from_secs(10),
            ws.send(Message::Text(identify.to_string().into())),
        )
        .await
        .map_or(true, |r| r.is_err())
        {
            attempt = attempt.saturating_add(1);
            continue;
        }
        let mut heartbeat = tokio::time::interval_at(
            Instant::now() + Duration::from_millis(interval / 2),
            Duration::from_millis(interval),
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut awaiting = false;
        let mut ready = false;
        let readiness = Instant::now() + Duration::from_secs(30);
        let mut guard = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _=guard.tick()=>{
                    if !ready && Instant::now()>readiness {break}
                    if calls.active.as_ref().is_some_and(|a|a.deadline.is_some_and(|deadline|Instant::now()>deadline))
                        || calls.streams.values().any(|s|s.deadline.is_some_and(|deadline|Instant::now()>deadline)) {
                        emit(&events,Event::Error(Error::Timeout))?;
                        break;
                    }
                }
                _=heartbeat.tick()=>{
                    if awaiting {break}
                    if timeout(Duration::from_secs(10),ws.send(Message::Text(json!({"op":1,"d":sequence}).to_string().into()))).await.map_or(true,|r|r.is_err()){break}
                    awaiting=true;
                }
                request=commands.recv()=>{
                    let Some(request)=request else{return Ok(())};
                    if !ready && !matches!(request.command,Command::Shutdown){let _=request.reply.send(Err(Error::NotReady));continue}
                    let (packet,number,shutdown)=match calls.packet(request.command,owner.id) {
                        Ok(result)=>result, Err(error)=>{let _=request.reply.send(Err(error));continue}
                    };
                    if let Some(packet)=packet
                        && timeout(Duration::from_secs(10),ws.send(Message::Text(packet.to_string().into()))).await.map_or(true,|r|r.is_err()) {
                            let _=request.reply.send(Err(Error::Ambiguous));break
                        }
                    let _=request.reply.send(Ok(number));
                    if shutdown {let _=timeout(Duration::from_secs(2),ws.close(None)).await;return Ok(())}
                }
                frame=ws.next()=>{
                    let Some(Ok(frame))=frame else{break};
                    let packet:Value=match frame {
                        Message::Text(text)=>serde_json::from_str(&text).map_err(|_|Error::Protocol)?,
                        Message::Ping(bytes)=>{if timeout(Duration::from_secs(10),ws.send(Message::Pong(bytes))).await.map_or(true,|r|r.is_err()){break}continue},
                        Message::Pong(_)=>continue,
                        Message::Close(close)=>{
                            if let Some(close)=close {
                                let code=u16::from(close.code);
                                if matches!(code,4004|4010..=4014){return Err(Error::Unauthorized)}
                                if matches!(code,4007|4009){session=None;sequence=None;resume_url=initial_url.clone();}
                            }
                            break;
                        }
                        _=>return Err(Error::Protocol),
                    };
                    if let Some(seq)=packet["s"].as_u64(){sequence=Some(seq)}
                    match packet["op"].as_u64().ok_or(Error::Protocol)? {
                        11=>awaiting=false,
                        1=>{if timeout(Duration::from_secs(10),ws.send(Message::Text(json!({"op":1,"d":sequence}).to_string().into()))).await.map_or(true,|r|r.is_err()){break}awaiting=true;},
                        7=>break,
                        9=>{if packet["d"]!=true {session=None;sequence=None;resume_url=initial_url.clone();}break},
                        0=>{
                            let name=packet["t"].as_str().ok_or(Error::Protocol)?;
                            let data=&packet["d"];
                            if name=="READY" {
                                let user:User=serde_json::from_value(data["user"].clone()).map_err(|_|Error::Protocol)?;
                                if user.id!=owner.id || user.bot {return Err(Error::Unauthorized)}
                                session=Some(crate::gateway::secret(data,"session_id")?);
                                if let Some(url)=data["resume_gateway_url"].as_str(){resume_url=validate_url(url)?;}
                                let channels:Vec<Channel>=serde_json::from_value(data.get("private_channels").cloned().unwrap_or(json!([]))).map_err(|_|Error::Protocol)?;
                                let mut guilds=Vec::new();
                                if let Some(values)=data["guilds"].as_array(){
                                    if values.len()>4096{return Err(Error::Capacity)}
                                    for g in values {
                                        let name=g["name"].as_str().or_else(||g["properties"]["name"].as_str()).unwrap_or("Unavailable");
                                        guilds.push(Guild{id:id(g,"id")?,name:name.chars().take(256).collect()});
                                    }
                                }
                                if channels.len()>4096{return Err(Error::Capacity)}
                                emit(&events,Event::Ready{user,channels,guilds})?;
                                ready=true;attempt=0;
                            } else if name=="RESUMED" {ready=true;attempt=0;emit(&events,Event::Resumed)?;}
                            else if ready {calls.dispatch(name,data,owner.id,&events)?;}
                            if ready
                                && let Some(guild)=departure.take()
                                    && timeout(Duration::from_secs(10),ws.send(Message::Text(voice_packet(None,guild,Controls{muted:true,deafened:true,camera:false}).to_string().into()))).await.map_or(true,|r|r.is_err()){break}
                        }
                        _=>{},
                    }
                }
            }
        }
        attempt = attempt.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn credential_order_departure_and_stale_events() {
        let (send, mut receive) = mpsc::channel(16);
        let mut calls = Calls::default();
        let target = CallTarget {
            channel: Id(10),
            guild: Some(Id(20)),
            peer: None,
        };
        let (packet, number, _) = calls
            .packet(Command::Join(target, Controls::default()), Id(1))
            .unwrap();
        assert_eq!(packet.unwrap()["d"]["channel_id"], "10");
        assert!(matches!(
            calls.packet(Command::Join(target, Controls::default()), Id(1)),
            Err(Error::Busy)
        ));
        calls
            .dispatch(
                "VOICE_SERVER_UPDATE",
                &json!({"guild_id":"20","token":"voice-token","endpoint":"voice.discord.media"}),
                Id(1),
                &send,
            )
            .unwrap();
        assert!(receive.try_recv().is_err());
        calls.dispatch("VOICE_STATE_UPDATE",&json!({"guild_id":"20","channel_id":"10","user_id":"1","session_id":"voice-session"}),Id(1),&send).unwrap();
        assert!(matches!(receive.try_recv().unwrap(), Event::Participant(_)));
        let Event::VoiceReady(connection) = receive.try_recv().unwrap() else {
            panic!("no voice credentials")
        };
        assert_eq!(connection.request, number);
        assert_eq!(connection.channel, Id(10));
        let (packet, _, _) = calls.packet(Command::Leave, Id(1)).unwrap();
        assert!(packet.unwrap()["d"]["channel_id"].is_null());
        assert!(matches!(
            calls.packet(Command::Join(target, Controls::default()), Id(1)),
            Err(Error::Busy)
        ));
        calls
            .dispatch(
                "VOICE_SERVER_UPDATE",
                &json!({"guild_id":"20","token":"late-token","endpoint":"voice.discord.media"}),
                Id(1),
                &send,
            )
            .unwrap();
        assert!(receive.try_recv().is_err());
        calls
            .dispatch(
                "VOICE_STATE_UPDATE",
                &json!({"guild_id":"20","channel_id":null,"user_id":"1"}),
                Id(1),
                &send,
            )
            .unwrap();
        assert!(matches!(receive.try_recv().unwrap(),Event::Left{request}if request==number));
        assert!(calls.active.is_none());
        assert!(
            calls
                .packet(Command::Join(target, Controls::default()), Id(1))
                .is_ok()
        );
    }
    #[test]
    fn stream_allocation_pairs_only_requested_keys() {
        let (send, mut receive) = mpsc::channel(16);
        let mut calls = Calls::default();
        let target = CallTarget {
            channel: Id(10),
            guild: None,
            peer: Some(Id(2)),
        };
        calls
            .packet(Command::Join(target, Controls::default()), Id(1))
            .unwrap();
        calls
            .dispatch(
                "VOICE_STATE_UPDATE",
                &json!({"channel_id":"10","user_id":"1","session_id":"session"}),
                Id(1),
                &send,
            )
            .unwrap();
        receive.try_recv().unwrap();
        let (packet, _, _) = calls.packet(Command::WatchStream(Id(2)), Id(1)).unwrap();
        assert_eq!(
            packet.unwrap(),
            json!({"op":20,"d":{"stream_key":"call:10:2"}})
        );
        calls.dispatch("STREAM_SERVER_UPDATE",&json!({"stream_key":"call:10:2","endpoint":"voice.discord.media","token":"stream-token"}),Id(1),&send).unwrap();
        assert!(receive.try_recv().is_err());
        calls
            .dispatch(
                "STREAM_CREATE",
                &json!({"stream_key":"call:10:2","rtc_server_id":"40","rtc_channel_id":"41"}),
                Id(1),
                &send,
            )
            .unwrap();
        let Event::StreamReady { key, connection } = receive.try_recv().unwrap() else {
            panic!("no stream")
        };
        assert_eq!(key, "call:10:2");
        assert_eq!(connection.guild, Some(Id(40)));
        assert_eq!(connection.channel, Id(41));
        let (packet, _, _) = calls.packet(Command::StopStream(key), Id(1)).unwrap();
        assert_eq!(packet.unwrap()["op"], 19);
        assert!(matches!(
            calls.packet(Command::WatchStream(Id(2)), Id(1)),
            Err(Error::Busy)
        ));
        // Late allocation must not revive the cancelled stream.
        calls.dispatch("STREAM_SERVER_UPDATE",&json!({"stream_key":"call:10:2","endpoint":"voice.discord.media","token":"late-token"}),Id(1),&send).unwrap();
        calls
            .dispatch(
                "STREAM_CREATE",
                &json!({"stream_key":"call:10:2","rtc_server_id":"50","rtc_channel_id":"51"}),
                Id(1),
                &send,
            )
            .unwrap();
        assert!(receive.try_recv().is_err());
        calls
            .dispatch(
                "STREAM_DELETE",
                &json!({"stream_key":"call:10:2"}),
                Id(1),
                &send,
            )
            .unwrap();
        assert!(matches!(
            receive.try_recv().unwrap(),
            Event::StreamEnded { .. }
        ));
        assert!(calls.packet(Command::WatchStream(Id(2)), Id(1)).is_ok());
    }
    #[test]
    fn call_updates_distinguish_omitted_fields_from_empty_lists() {
        let (send, mut receive) = mpsc::channel(4);
        let mut calls = Calls::default();
        calls
            .dispatch(
                "CALL_UPDATE",
                &json!({"channel_id":"10","ringing":[]}),
                Id(1),
                &send,
            )
            .unwrap();
        assert!(
            matches!(receive.try_recv().unwrap(),Event::Call{ringing:Some(ringing),participants:None,..}if ringing.is_empty())
        );
        calls
            .dispatch(
                "CALL_UPDATE",
                &json!({"channel_id":"10","voice_states":[]}),
                Id(1),
                &send,
            )
            .unwrap();
        assert!(
            matches!(receive.try_recv().unwrap(),Event::Call{ringing:None,participants:Some(participants),..}if participants.is_empty())
        );
    }
    #[test]
    fn gateway_destinations_and_queue_capacity_are_checked() {
        assert!(validate_url("wss://gateway.discord.gg").is_ok());
        for bad in [
            "ws://gateway.discord.gg",
            "wss://gateway.discord.gg.evil.test",
            "wss://user@gateway.discord.gg",
            "wss://gateway.discord.gg:444",
        ] {
            assert!(validate_url(bad).is_err());
        }
        let (send, _receive) = mpsc::channel(1);
        emit(&send, Event::Resumed).unwrap();
        assert!(matches!(emit(&send, Event::Resumed), Err(Error::Capacity)));
    }
    #[tokio::test]
    async fn local_gateway_join_controls_and_leave() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(Message::Text(
                json!({"op":10,"d":{"heartbeat_interval":10000}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let identify: Value =
                serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
            assert_eq!(identify["op"], 2);
            ws.send(Message::Text(json!({"op":0,"t":"READY","s":1,"d":{"session_id":"synthetic-session","user":{"id":"1","username":"synthetic"},"private_channels":[],"guilds":[]}}).to_string().into())).await.unwrap();
            let join: Value =
                serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
            assert_eq!(join["op"], 4);
            assert_eq!(join["d"]["channel_id"], "10");
            for (name, data) in [
                (
                    "VOICE_STATE_UPDATE",
                    json!({"user_id":"1","channel_id":"10","session_id":"voice-session"}),
                ),
                (
                    "VOICE_SERVER_UPDATE",
                    json!({"channel_id":"10","token":"synthetic-voice","endpoint":"voice.discord.media"}),
                ),
            ] {
                ws.send(Message::Text(
                    json!({"op":0,"t":name,"s":2,"d":data}).to_string().into(),
                ))
                .await
                .unwrap();
            }
            let controls: Value =
                serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
            assert_eq!(controls["d"]["self_video"], true);
            assert_eq!(controls["d"]["self_mute"], true);
            let leave: Value =
                serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
            assert!(leave["d"]["channel_id"].is_null());
            ws.send(Message::Text(json!({"op":0,"t":"VOICE_STATE_UPDATE","s":3,"d":{"user_id":"1","channel_id":null}}).to_string().into())).await.unwrap();
            assert!(matches!(
                ws.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
        });
        let secret = Arc::new(Secret::new("synthetic-token".into()).unwrap());
        let owner = User {
            id: Id(1),
            username: "synthetic".into(),
            global_name: None,
            bot: false,
        };
        let (commands, receive) = mpsc::channel(8);
        let (send, mut events) = mpsc::channel(16);
        let handle = SessionHandle {
            commands,
            api: Arc::new(Api::new(secret.clone()).unwrap()),
            owner: owner.id,
        };
        let worker = tokio::spawn(async move {
            run(
                secret,
                owner,
                "wss://gateway.discord.gg/?v=10&encoding=json".into(),
                receive,
                send,
                Some(&endpoint),
            )
            .await
        });
        assert!(matches!(events.recv().await.unwrap(), Event::Ready { .. }));
        handle
            .join(
                CallTarget {
                    channel: Id(10),
                    guild: None,
                    peer: Some(Id(2)),
                },
                Controls::default(),
            )
            .await
            .unwrap();
        while !matches!(events.recv().await.unwrap(), Event::VoiceReady(_)) {}
        handle
            .set_controls(Controls {
                muted: true,
                camera: true,
                deafened: false,
            })
            .await
            .unwrap();
        handle.leave().await.unwrap();
        while !matches!(events.recv().await.unwrap(), Event::Left { .. }) {}
        handle.shutdown().await.unwrap();
        timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }
}
