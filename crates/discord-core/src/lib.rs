//! Headless Discord calling with caller-owned audio/video inputs and outputs.
//!
//! [`Client`] coordinates session signaling, call media, camera video and Go Live
//! streams. It owns no window or device. Retain it while navigating application pages.
pub use discord_media as media;
pub use discord_session as session;
pub use session::{CallTarget, Controls, Error, Id, Secret};
use std::{
    sync::{Arc, mpsc as sync},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

/// Media supplied by the application. Audio is mono f32, 48 kHz, 20 ms per frame.
/// Camera input is complete H.264 access units, currently 640x480 at 15 fps.
pub struct MediaIo {
    pub capture: sync::Receiver<media::Frame>,
    pub playback: sync::SyncSender<media::Frame>,
    pub camera: Option<sync::Receiver<media::camera_video::Frame>>,
    pub remote_video: Option<media::VideoSink>,
}
/// The application side of the bounded queues returned by [`media_channels`].
pub struct MediaChannels {
    pub microphone: sync::SyncSender<media::Frame>,
    pub speaker: sync::Receiver<media::Frame>,
    pub camera: sync::SyncSender<media::camera_video::Frame>,
}
/// Allocate bounded media queues: 160 ms of audio and two encoded camera pictures.
/// Video callbacks run on decoder workers; copy borrowed pixels before returning.
pub fn media_channels(remote_video: Option<media::VideoSink>) -> (MediaChannels, MediaIo) {
    let (microphone, capture) = sync::sync_channel(8);
    let (playback, speaker) = sync::sync_channel(8);
    let (camera, frames) = sync::sync_channel(2);
    (
        MediaChannels {
            microphone,
            speaker,
            camera,
        },
        MediaIo {
            capture,
            playback,
            camera: Some(frames),
            remote_video,
        },
    )
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Phase {
    #[default]
    Idle,
    Signaling,
    ConnectingMedia,
    Securing,
    WaitingForPeer,
    Connected,
    Leaving,
    Failed,
}
#[derive(Clone, Debug, Default)]
pub struct State {
    pub phase: Phase,
    pub target: Option<CallTarget>,
    pub controls: Controls,
    /// Restrictions imposed by the server, independent of local preferences.
    pub server_muted: bool,
    pub server_deafened: bool,
    /// Attach this generation to outgoing camera frames; zero means camera disabled.
    pub camera_generation: u64,
    pub privacy_code: Option<String>,
    pub last_error: Option<String>,
}
#[derive(Debug)]
pub enum Event {
    Signaling(session::Event),
    Speaking(Vec<Id>),
    CameraAvailable(bool),
    /// A stream has reached encrypted media readiness.
    StreamStarted(String),
    /// Stream transport progress; allocation alone is not usable media.
    StreamState {
        key: String,
        phase: Phase,
    },
    StreamStopped(String),
    Failure(String),
}

enum Command {
    Join {
        target: CallTarget,
        io: MediaIo,
        ring: bool,
        controls: Controls,
    },
    Leave,
    Controls(Controls),
    Share(media::screen::Video),
    Watch {
        user: Id,
        sink: media::VideoSink,
    },
    StopSharing,
    StopWatching,
    Shutdown,
}
struct Request {
    command: Command,
    reply: oneshot::Sender<Result<(), Error>>,
}
#[derive(Clone)]
pub struct Handle {
    commands: mpsc::Sender<Request>,
    session: session::SessionHandle,
    state: watch::Receiver<State>,
}
impl Handle {
    async fn send(&self, command: Command) -> Result<(), Error> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Request { command, reply })
            .await
            .map_err(|_| Error::Closed)?;
        tokio::time::timeout(Duration::from_secs(30), receive)
            .await
            .map_err(|_| Error::Ambiguous)?
            .map_err(|_| Error::Closed)?
    }
    pub fn state(&self) -> watch::Receiver<State> {
        self.state.clone()
    }
    pub fn api(&self) -> &Arc<session::Api> {
        self.session.api()
    }
    /// Join a server channel or existing DM call without ringing anyone.
    pub async fn join(
        &self,
        target: CallTarget,
        io: MediaIo,
        controls: Controls,
    ) -> Result<(), Error> {
        self.send(Command::Join {
            target,
            io,
            ring: false,
            controls,
        })
        .await
    }
    /// Start a DM/group call, ringing once after server allocation. Never retried automatically.
    pub async fn call(
        &self,
        target: CallTarget,
        io: MediaIo,
        controls: Controls,
    ) -> Result<(), Error> {
        if target.guild.is_some() {
            return Err(Error::Protocol);
        }
        self.send(Command::Join {
            target,
            io,
            ring: true,
            controls,
        })
        .await
    }
    /// Answer is the same media flow as join; it never emits a new ring.
    pub async fn answer(
        &self,
        target: CallTarget,
        io: MediaIo,
        controls: Controls,
    ) -> Result<(), Error> {
        self.join(target, io, controls).await
    }
    pub async fn decline(&self, channel: Id) -> Result<(), Error> {
        self.api().stop_ringing(channel, None).await
    }
    pub async fn sync_call(&self, channel: Id) -> Result<(), Error> {
        self.session.sync_call(channel).await
    }
    pub async fn leave(&self) -> Result<(), Error> {
        self.send(Command::Leave).await
    }
    pub async fn set_controls(&self, controls: Controls) -> Result<(), Error> {
        self.send(Command::Controls(controls)).await
    }
    pub async fn share(&self, video: media::screen::Video) -> Result<(), Error> {
        self.send(Command::Share(video)).await
    }
    pub async fn watch(&self, user: Id, sink: media::VideoSink) -> Result<(), Error> {
        self.send(Command::Watch { user, sink }).await
    }
    pub async fn stop_sharing(&self) -> Result<(), Error> {
        self.send(Command::StopSharing).await
    }
    pub async fn stop_watching(&self) -> Result<(), Error> {
        self.send(Command::StopWatching).await
    }
}

/// Owns all tasks. `close` sends departure; dropping cancels local work immediately.
pub struct Client {
    handle: Handle,
    events: mpsc::Receiver<Event>,
    worker: JoinHandle<()>,
}
impl Client {
    pub async fn connect(secret: Secret) -> Result<Self, Error> {
        let session = session::Session::connect(secret).await?;
        let signaling = session.handle();
        let (commands, receive) = mpsc::channel(16);
        let (events, receiver) = mpsc::channel(128);
        let (state, changes) = watch::channel(State::default());
        let worker = tokio::spawn(async move {
            controller(session, receive, events, state).await;
        });
        Ok(Self {
            handle: Handle {
                commands,
                session: signaling,
                state: changes,
            },
            events: receiver,
            worker,
        })
    }
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    pub fn state(&self) -> watch::Receiver<State> {
        self.handle.state()
    }
    pub async fn next_event(&mut self) -> Option<Event> {
        self.events.recv().await
    }
    pub async fn close(mut self) -> Result<(), Error> {
        let result = self.handle.send(Command::Shutdown).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut self.worker).await;
        result
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.worker.abort();
    }
}
struct Task(JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Active {
    request: u64,
    target: CallTarget,
    io: Option<MediaIo>,
    ring: bool,
    camera_serial: u64,
    controls: watch::Sender<media::Controls>,
    identity: Arc<media::Identity>,
    task: Option<Task>,
    stream_audio: sync::SyncSender<media::Frame>,
    stream_playback: Option<sync::Receiver<media::Frame>>,
    share_serial: u64,
    watch_serial: u64,
    share: Option<media::screen::Video>,
    share_key: Option<String>,
    share_task: Option<Task>,
    watch: Option<(Id, media::VideoSink)>,
    watch_key: Option<String>,
    watch_task: Option<Task>,
}
impl Active {
    // A cancelled worker may already have queued an update. Match the stream
    // generation as well as the call so it cannot terminate a replacement.
    fn accepts(&self, request: u64, kind: Kind) -> bool {
        self.request == request
            && match kind {
                Kind::Call => self.task.is_some(),
                Kind::Share(serial) => self.share_task.is_some() && self.share_serial == serial,
                Kind::Watch(serial) => self.watch_task.is_some() && self.watch_serial == serial,
            }
    }
}
#[derive(Clone, Copy)]
enum Kind {
    Call,
    Share(u64),
    Watch(u64),
}
enum Update {
    Status(u64, Kind, media::Status),
    End(u64, Kind, Result<(), &'static str>),
}
fn status_sink(
    send: mpsc::Sender<Update>,
    request: u64,
    kind: Kind,
) -> impl Fn(media::Status) -> Result<(), ()> + Send + 'static {
    move |status| {
        send.try_send(Update::Status(request, kind, status))
            .map_err(|_| ())
    }
}
fn event(send: &mpsc::Sender<Event>, value: Event) -> bool {
    send.try_send(value).is_ok()
}
fn fail(state: &watch::Sender<State>, events: &mpsc::Sender<Event>, message: String) {
    state.send_modify(|s| {
        s.phase = Phase::Failed;
        s.privacy_code = None;
        s.last_error = Some(message.clone());
    });
    let _ = events.try_send(Event::Failure(message));
}
async fn controller(
    mut session: session::Session,
    mut commands: mpsc::Receiver<Request>,
    events: mpsc::Sender<Event>,
    state: watch::Sender<State>,
) {
    let handle = session.handle();
    let (updates, mut receive) = mpsc::channel(128);
    let mut active: Option<Active> = None;
    loop {
        tokio::select! {
            request=commands.recv()=>{
                let Some(request)=request else{break};
                let result=match request.command {
                    Command::Join{target,io,ring,controls}=>{
                        if active.is_some(){Err(Error::Busy)}else{
                            match handle.join(target,controls).await {
                                Err(error)=>Err(error),
                                Ok(number)=>{
                                    let initial=media::Controls{muted:controls.muted||controls.deafened,deafened:controls.deafened,camera:u64::from(controls.camera),..Default::default()};
                                    let (controls_send,_)=watch::channel(initial);
                                    let (audio,playback)=sync::sync_channel(8);
                                    active=Some(Active{request:number,target,io:Some(io),ring,camera_serial:u64::from(controls.camera),controls:controls_send,identity:media::Identity::generate(),task:None,stream_audio:audio,stream_playback:Some(playback),share_serial:0,watch_serial:0,share:None,share_key:None,share_task:None,watch:None,watch_key:None,watch_task:None});
                                    state.send_replace(State{phase:Phase::Signaling,target:Some(target),controls,camera_generation:u64::from(controls.camera),..Default::default()});
                                    Ok(())
                                }
                            }
                        }
                    }
                    Command::Leave=>{
                        active=None; // Cancel local media before waiting for departure.
                        state.send_modify(|s|{s.phase=if s.target.is_some(){Phase::Leaving}else{Phase::Idle};s.privacy_code=None;});
                        handle.leave().await
                    }
                    Command::Controls(controls)=>{
                        if let Some(active)=active.as_mut(){
                            // Local mute/deafen takes effect even if the service write fails.
                            let previous=state.borrow().clone();
                            let generation=if controls.camera {
                                if previous.controls.camera{previous.camera_generation}else{active.camera_serial.saturating_add(1).max(1)}
                            }else{0};
                            active.camera_serial=active.camera_serial.max(generation);
                            active.controls.send_modify(|c|{c.muted=controls.muted||controls.deafened||previous.server_muted||previous.server_deafened;c.deafened=controls.deafened||previous.server_deafened;c.camera=generation;});
                            state.send_modify(|s|{s.controls=controls;s.camera_generation=generation;});
                            handle.set_controls(controls).await
                        }else{Err(Error::NotReady)}
                    }
                    Command::Share(video)=>{
                        if let Some(active)=active.as_mut().filter(|a|a.task.is_some()&&state.borrow().phase==Phase::Connected){
                            if active.share.is_some()||active.share_key.is_some(){Err(Error::Busy)}else{
                                match handle.start_stream().await{Ok(())=>{active.share_serial+=1;active.share_key=Some(stream_key(active.target,handle.user_id()));active.share=Some(video);Ok(())},Err(e)=>Err(e)}
                            }
                        }else{Err(Error::NotReady)}
                    }
                    Command::Watch{user,sink}=>{
                        if let Some(active)=active.as_mut().filter(|a|a.task.is_some()&&state.borrow().phase==Phase::Connected){
                            if active.watch.is_some()||active.watch_key.is_some(){Err(Error::Busy)}else{match handle.watch_stream(user).await{Ok(())=>{active.watch_serial+=1;active.watch_key=Some(stream_key(active.target,user));active.watch=Some((user,sink));Ok(())},Err(e)=>Err(e)}}
                        }else{Err(Error::NotReady)}
                    }
                    Command::StopSharing=>{
                        if let Some(active)=active.as_mut(){
                            active.share=None;active.share_task=None;
                            if let Some(key)=active.share_key.clone(){handle.stop_stream(key).await}else{Err(Error::NotReady)}
                        }else{Err(Error::NotReady)}
                    }
                    Command::StopWatching=>{
                        if let Some(active)=active.as_mut(){
                            active.watch=None;active.watch_task=None;
                            if let Some(key)=active.watch_key.clone(){handle.stop_stream(key).await}else{Err(Error::NotReady)}
                        }else{Err(Error::NotReady)}
                    }
                    Command::Shutdown=>{
                        drop(active.take());
                        let result=handle.shutdown().await;
                        let _=request.reply.send(result);break
                    }
                };
                let _=request.reply.send(result);
            }
            incoming=session.next_event()=>{
                let Some(incoming)=incoming else{fail(&state,&events,"Session closed".into());break};
                match incoming {
                    session::Event::VoiceReady(connection)=>{
                        let Some(call)=active.as_mut().filter(|a|a.request==connection.request)else{continue};
                        let Some(io)=call.io.take()else{
                            active=None;fail(&state,&events,"Voice server changed; rejoin the call".into());let _=handle.leave().await;continue
                        };
                        let ring=std::mem::take(&mut call.ring);
                        let api=handle.api().clone();let channel=call.target.channel;
                        let send=updates.clone();let number=call.request;let identity=call.identity.clone();let controls=call.controls.subscribe();let stream_audio=call.stream_playback.take();
                        call.task=Some(Task(tokio::spawn(async move{
                            // REST can take seconds. Keep it cancellable with this call
                            // so leave/mute commands do not wait behind a ring request.
                            let result=async {
                                if ring {
                                    api.ring(channel,None).await.map_err(|_|"Ring request failed; it may already have reached Discord")?;
                                }
                                media::run_with_identity(connection,io.capture,io.playback,controls,io.camera,io.remote_video,stream_audio,status_sink(send.clone(),number,Kind::Call),identity).await
                            }.await;
                            let _=send.send(Update::End(number,Kind::Call,result)).await;
                        })));
                        state.send_modify(|s|s.phase=Phase::ConnectingMedia);
                    }
                    session::Event::VoiceUnavailable{request}=>{
                        if active.as_ref().is_some_and(|a|a.request==request){
                            active=None;
                            fail(&state,&events,"Voice server unavailable; rejoin the call".into());
                            let _=handle.leave().await;
                        }
                    }
                    session::Event::Participant(participant)=>{
                        if participant.user_id==handle.user_id()
                            && let Some(call)=active.as_mut().filter(|a|Some(a.target.channel)==participant.channel_id&&a.target.guild==participant.guild_id) {
                            let local=state.borrow().controls;
                            call.controls.send_modify(|c|{
                                c.muted=local.muted||local.deafened||participant.mute||participant.deaf;
                                c.deafened=local.deafened||participant.deaf;
                            });
                            state.send_modify(|s|{s.server_muted=participant.mute;s.server_deafened=participant.deaf;});
                        }
                        if !event(&events,Event::Signaling(session::Event::Participant(participant))){break}
                    }
                    session::Event::StreamReady{key,connection}=>{
                        let Some(call)=active.as_mut().filter(|a|a.request==connection.request)else{continue};
                        let send=updates.clone();let identity=call.identity.clone();let number=call.request;
                        let streamer=key.rsplit(':').next().and_then(|s|s.parse::<u64>().ok()).map(Id);
                        if streamer==Some(connection.user){
                            let Some(video)=call.share.take()else{continue};
                            if call.share_key.as_ref()!=Some(&key){continue}
                            let kind=Kind::Share(call.share_serial);
                            call.share_task=Some(Task(tokio::spawn(async move{
                                let result=media::run_stream(connection,identity,video,status_sink(send.clone(),number,kind)).await;
                                let _=send.send(Update::End(number,kind,result)).await;
                            })));
                        }else if let Some((user,sink))=&call.watch {
                            if Some(*user)!=streamer{continue}
                            let sink=sink.clone();let audio=call.stream_audio.clone();
                            if call.watch_key.as_ref()!=Some(&key){continue}
                            let kind=Kind::Watch(call.watch_serial);
                            call.watch_task=Some(Task(tokio::spawn(async move{
                                let result=media::watch_stream(connection,identity,sink,Some(audio),status_sink(send.clone(),number,kind)).await;
                                let _=send.send(Update::End(number,kind,result)).await;
                            })));
                        }else{continue}
                        if !event(&events,Event::StreamState{key,phase:Phase::ConnectingMedia}){break}
                    }
                    session::Event::StreamEnded{key}=>{
                        if let Some(call)=active.as_mut(){
                            if call.share_key.as_ref()==Some(&key){call.share_task=None;call.share_key=None;call.share=None;}
                            if call.watch_key.as_ref()==Some(&key){call.watch_task=None;call.watch_key=None;call.watch=None;}
                        }
                        if !event(&events,Event::StreamStopped(key)){break}
                    }
                    session::Event::Left{request}=>{
                        if active.as_ref().is_none_or(|a|a.request==request){active=None;state.send_replace(State::default());}
                        if !event(&events,Event::Signaling(session::Event::Left{request})){break}
                    }
                    session::Event::Disconnected=>{
                        if active.take().is_some(){fail(&state,&events,"Gateway disconnected; rejoin when ready".into());}
                        if !event(&events,Event::Signaling(session::Event::Disconnected)){break}
                    }
                    session::Event::Error(error)=>{active=None;fail(&state,&events,error.to_string());}
                    other=>{if !event(&events,Event::Signaling(other)){break}}
                }
            }
            update=receive.recv()=>{
                let Some(update)=update else{break};
                match update{
                    Update::Status(number,kind,status)=>{
                        if active.as_ref().is_none_or(|a|!a.accepts(number,kind)){continue}
                        if !matches!(kind,Kind::Call){
                            let call=active.as_ref().expect("update matched active call");
                            let key=match kind {Kind::Share(_)=>call.share_key.clone(),Kind::Watch(_)=>call.watch_key.clone(),Kind::Call=>None};
                            let Some(key)=key else{continue};
                            let phase=match status {
                                media::Status::Connecting|media::Status::Discovering|media::Status::TransportReady=>Phase::ConnectingMedia,
                                media::Status::Securing=>Phase::Securing,
                                media::Status::WaitingForPeer=>Phase::WaitingForPeer,
                                media::Status::Ready{..}=>Phase::Connected,
                                _=>continue,
                            };
                            if phase==Phase::Connected&&!event(&events,Event::StreamStarted(key.clone())){break}
                            if !event(&events,Event::StreamState{key,phase}){break}
                            continue
                        }
                        match status{
                            media::Status::Connecting|media::Status::Discovering|media::Status::TransportReady=>state.send_modify(|s|s.phase=Phase::ConnectingMedia),
                            media::Status::Securing=>state.send_modify(|s|{s.phase=Phase::Securing;s.privacy_code=None;}),
                            media::Status::WaitingForPeer=>state.send_modify(|s|{s.phase=Phase::WaitingForPeer;s.privacy_code=None;}),
                            media::Status::Ready{privacy_code}=>state.send_modify(|s|{s.phase=Phase::Connected;s.privacy_code=Some(privacy_code);}),
                            media::Status::RemoteAudio=>{},
                            media::Status::Speaking(users)=>{if !event(&events,Event::Speaking(users.iter().copied().filter(|n|*n!=0).map(Id).collect())){break}},
                            media::Status::CameraAvailable(available)=>{if !event(&events,Event::CameraAvailable(available)){break}},
                        }
                    }
                    Update::End(number,kind,result)=>{
                        let Some(call)=active.as_mut().filter(|a|a.accepts(number,kind))else{continue};
                        match kind{
                            Kind::Call=>{active=None;fail(&state,&events,result.err().unwrap_or("Call transport closed").into());let _=handle.leave().await;},
                            Kind::Share(_)=>{call.share_task=None;call.share=None;if let Some(key)=call.share_key.clone(){let _=handle.stop_stream(key).await;}},
                            Kind::Watch(_)=>{call.watch_task=None;call.watch=None;if let Some(key)=call.watch_key.clone(){let _=handle.stop_stream(key).await;}},
                        }
                        if let Err(message)=result{let _=event(&events,Event::Failure(message.into()));}
                    }
                }
            }
        }
    }
    drop(active);
}

fn stream_key(target: CallTarget, user: Id) -> String {
    target.guild.map_or_else(
        || format!("call:{}:{user}", target.channel),
        |g| format!("guild:{g}:{}:{user}", target.channel),
    )
}
