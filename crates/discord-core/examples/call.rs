//! Explicitly joins an existing call with local audio and optional camera.
//! Credentials come from DISCORD_TOKEN. Nothing is persisted or logged.
use discord_core::{
    CallTarget, Client, Controls, Id, Phase, Secret, media, media_channels, session,
};
use std::{env, error::Error, sync::Arc, time::Instant};

fn optional_id(name: &str) -> Result<Option<Id>, Box<dyn Error>> {
    env::var(name)
        .ok()
        .map(|s| {
            let id = s.parse::<u64>()?;
            if id == 0 {
                return Err("IDs must be nonzero".into());
            }
            Ok(Id(id))
        })
        .transpose()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let token = Secret::new(env::var("DISCORD_TOKEN")?)?;
    let channel = optional_id("DISCORD_CHANNEL")?.ok_or("Set DISCORD_CHANNEL")?;
    let target = CallTarget {
        channel,
        guild: optional_id("DISCORD_GUILD")?,
        peer: optional_id("DISCORD_PEER")?,
    };
    let mut client = Client::connect(token).await?;
    tokio::time::timeout(std::time::Duration::from_secs(45), async {
        loop {
            match client.next_event().await {
                Some(discord_core::Event::Signaling(session::Event::Ready { .. })) => return Ok(()),
                Some(discord_core::Event::Failure(message)) => return Err(message),
                None => return Err("Session closed before ready".into()),
                _ => {}
            }
        }
    })
    .await??;
    let handle = client.handle();
    let mut state = client.state();
    // Replace this callback with bounded delivery to your renderer. Pixels are
    // borrowed for the callback only; never retain the slice or block this worker.
    let sink: media::VideoSink = Arc::new(|_frame| {});
    let (channels, io) = media_channels(Some(sink));
    let audio = call_media::audio::Audio::start(
        call_media::audio::Devices {
            input: env::var("AUDIO_INPUT").ok(),
            output: env::var("AUDIO_OUTPUT").ok(),
        },
        channels.microphone,
        channels.speaker,
        |result| {
            if let Err(message) = result {
                eprintln!("Audio: {message}");
            }
        },
    )?;
    let camera_device = env::var("CAMERA_DEVICE").ok();
    let camera_enabled = camera_device.is_some();
    let camera = if camera_enabled {
        let camera_state = state.clone();
        let started = Instant::now();
        Some(call_media::camera::Camera::start(
            camera_device,
            Arc::new(move |frame| {
                let state = camera_state.borrow();
                if state.phase != Phase::Connected || state.camera_generation == 0 {
                    return;
                }
                let _ = channels.camera.try_send(media::camera_video::Frame {
                    generation: state.camera_generation,
                    timestamp: (started.elapsed().as_micros() * 90 / 1000) as u32,
                    data: frame.h264,
                });
            }),
            Arc::new(|| {}),
        )?)
    } else {
        None
    };
    handle
        .join(
            target,
            io,
            Controls {
                camera: camera_enabled,
                ..Default::default()
            },
        )
        .await?;
    eprintln!(
        "Joining; press Ctrl-C to leave. Incoming video is decoded and discarded by this example."
    );
    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => { result?; break; }
            changed = state.changed() => {
                if changed.is_err() { break; }
                let state = state.borrow_and_update().clone();
                audio.set_ready(state.phase == Phase::Connected);
                audio.set_controls(state.controls.muted || state.server_muted, state.controls.deafened || state.server_deafened);
                eprintln!("Call: {:?}", state.phase);
                if matches!(state.phase, Phase::Failed | Phase::Idle) { break; }
            }
            event = client.next_event() => {
                match event {
                    Some(discord_core::Event::Failure(message)) => eprintln!("Call error: {message}"),
                    None => break,
                    _ => {} // Drain events without printing credential-bearing payloads.
                }
            }
        }
    }
    drop(camera);
    let done = audio.shutdown();
    let result = client.close().await;
    tokio::task::spawn_blocking(move || done.recv_timeout(std::time::Duration::from_secs(5)))
        .await??;
    result?;
    Ok(())
}
