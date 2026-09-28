//! Read-only device enumeration for configuring the call example.
fn main() {
    match call_media::audio::devices() {
        Ok(devices) => {
            println!("Microphones (AUDIO_INPUT):");
            for (id, label) in devices.inputs {
                println!("  {id:?}  {label}");
            }
            println!("Speakers (AUDIO_OUTPUT):");
            for (id, label) in devices.outputs {
                println!("  {id:?}  {label}");
            }
        }
        Err(error) => eprintln!("Audio enumeration: {error}"),
    }
    match call_media::camera::devices() {
        Ok(devices) => {
            println!("Cameras (CAMERA_DEVICE; /dev/videoN also identifies SHARE_VIDEO_DEVICE=N):");
            for (id, label) in devices {
                println!("  {id:?}  {label}");
            }
        }
        Err(error) => eprintln!("Camera enumeration: {error}"),
    }
    println!(
        "Share audio uses a native PulseAudio source name or ALSA PCM name, not the microphone IDs above."
    );
}
