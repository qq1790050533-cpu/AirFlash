//! Delayed local monitoring so local speakers can be aligned with remote playback.
//!
//! AirPlay receivers render later than the captured Windows audio, so a local
//! device playing the same source is audible too early. The monitor renders the
//! streamed audio to a local output endpoint after a selectable delay, which
//! makes both paths audible at the same time.
//!
//! The monitor endpoint must differ from the capture endpoint. Otherwise the dry
//! source and the delayed copy overlap, and endpoint mute would silence the
//! monitor as well.
use anyhow::{Result, ensure};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};
use windows::{
    Win32::{
        Foundation::{CloseHandle, HANDLE, WAIT_OBJECT_0},
        Media::Audio::{Endpoints::IAudioEndpointVolume, *},
        System::{Com::*, Threading::*},
    },
};
/// Largest alignment delay accepted from the host, in milliseconds.
pub const MAX_DELAY_MS: u32 = 2000;
/// Ask the audio engine to resample the monitor stream to the device mix format.
const AUTOCONVERT_PCM: u32 = 0x8000_0000;
const SRC_DEFAULT_QUALITY: u32 = 0x0800_0000;
const IEEE_FLOAT: u16 = 0x0003;
/// Buffered audio beyond the target plus this tolerance is treated as drift.
const TRIM_TOLERANCE_MS: u32 = 50;
const CHANNELS: usize = 2;
/// Shared, host-adjustable alignment delay.
#[derive(Clone)]
pub struct Delay {
    ms: Arc<AtomicU32>,
}
impl Delay {
    pub fn new(ms: u32) -> Self {
        Self { ms: Arc::new(AtomicU32::new(ms.min(MAX_DELAY_MS))) }
    }
    pub fn set_ms(&self, ms: u32) {
        self.ms.store(ms.min(MAX_DELAY_MS), Ordering::Release);
    }
    pub fn ms(&self) -> u32 {
        self.ms.load(Ordering::Acquire)
    }
    pub fn frames(&self, rate: u32) -> usize {
        self.ms() as usize * rate as usize / 1000
    }
}
impl Default for Delay {
    fn default() -> Self {
        Self::new(0)
    }
}
/// Volume of the local output endpoint, mirrored from the Windows volume control
/// so the host can balance the speakers against the AirPlay receivers.
#[derive(Clone, Default)]
pub struct LocalVolume {
    /// Requested percent plus one; zero means nothing is pending.
    requested: Arc<AtomicU32>,
    /// Percent last read from or written to the endpoint, plus one; zero is unknown.
    applied: Arc<AtomicU32>,
    available: Arc<AtomicBool>,
}
impl LocalVolume {
    /// Ask the monitor thread to apply `percent` to the endpoint.
    pub fn set(&self, percent: u8) {
        self.requested
            .store(u32::from(percent.min(100)) + 1, Ordering::Release);
    }
    /// Percent reported by the endpoint, when it was readable.
    pub fn volume(&self) -> Option<u8> {
        match self.applied.load(Ordering::Acquire) {
            0 => None,
            value => Some((value - 1) as u8),
        }
    }
    /// Whether the endpoint exposes a volume control at all.
    pub fn available(&self) -> bool {
        self.available.load(Ordering::Acquire)
    }
    fn publish(&self, percent: u8) {
        self.applied
            .store(u32::from(percent) + 1, Ordering::Release);
        self.available.store(true, Ordering::Release);
    }
    fn pending(&self) -> Option<u8> {
        match self.requested.load(Ordering::Acquire) {
            0 => None,
            value => Some((value - 1) as u8),
        }
    }
    fn settle(&self) {
        self.requested.store(0, Ordering::Release);
    }
}
/// Interleaved stereo delay line. The line keeps `target` frames buffered, so
/// audio is rendered `target` frames after it was captured. Starvation renders
/// silence without consuming the buffered audio, which keeps the delay intact
/// when the capture stalls for a moment.
#[derive(Default)]
struct DelayLine {
    samples: VecDeque<f32>,
    dropped_frames: u64,
}
impl DelayLine {
    fn push(&mut self, samples: &[f32], limit: usize) {
        self.samples.extend(samples.iter().copied());
        self.trim(limit / 2, 0);
    }
    /// Trim buffered audio that exceeds `target + tolerance` frames.
    fn trim(&mut self, target: usize, tolerance: usize) {
        let limit = (target + tolerance) * CHANNELS;
        if self.samples.len() > limit {
            let excess = self.samples.len() - limit;
            self.samples.drain(..excess);
            self.dropped_frames += excess as u64 / CHANNELS as u64;
        }
    }
    /// Append `frames` stereo frames, playing only what is older than `target`
    /// frames and padding the rest with silence.
    fn fill(&mut self, frames: usize, target: usize, out: &mut Vec<f32>) {
        let held = target * CHANNELS;
        for _ in 0..frames {
            if self.samples.len() > held {
                out.push(self.samples.pop_front().unwrap_or(0.0));
                out.push(self.samples.pop_front().unwrap_or(0.0));
            } else {
                out.push(0.0);
                out.push(0.0);
            }
        }
    }
}
struct Shared {
    line: DelayLine,
}
struct ComGuard;
impl Drop for ComGuard {
    fn drop(&mut self) {
        unsafe { CoUninitialize() }
    }
}
struct AudioGuard(IAudioClient);
impl Drop for AudioGuard {
    fn drop(&mut self) {
        unsafe {
            let _ = self.0.Stop();
        }
    }
}
struct EventGuard(HANDLE);
impl Drop for EventGuard {
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.0);
        }
    }
}
pub struct Monitor {
    shared: Arc<Mutex<Shared>>,
    delay: Delay,
    volume: LocalVolume,
    limit: usize,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<()>>>,
}
impl Monitor {
    /// Render the streamed audio on `endpoint` (default render device when unset).
    pub fn start(endpoint: Option<String>, rate: u32, delay: Delay, volume: LocalVolume) -> Result<Self> {
        ensure!(
            (8000..=192_000).contains(&rate),
            "unsupported monitor sample rate"
        );
        let limit = rate as usize * 3 * CHANNELS;
        let shared = Arc::new(Mutex::new(Shared { line: DelayLine::default() }));
        let stop = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::sync_channel(1);
        let (worker_stop, worker_shared, worker_delay, worker_volume) = (stop.clone(), shared.clone(), delay.clone(), volume.clone());
        let worker = thread::Builder::new()
            .name("wasapi-monitor".into())
            .spawn(move || {
                let result = render(endpoint, rate, worker_delay, worker_stop, &worker_shared, &worker_volume, &tx);
                if let Err(e) = &result {
                    let _ = tx.try_send(Err(format!("{e:#}")));
                }
                result
            })?;
        match rx.recv_timeout(Duration::from_secs(3)) {
            Ok(Ok(())) => Ok(Self { shared, delay, volume, limit, stop, worker: Some(worker) }),
            other => {
                stop.store(true, Ordering::Release);
                let _ = worker.join();
                anyhow::bail!("local monitor initialization: {other:?}");
            }
        }
    }
    /// Buffer the audio that was sent to the receivers, big-endian S16 stereo.
    pub fn push_pcm(&self, bytes: &[u8]) {
        let mut samples = Vec::with_capacity(bytes.len() / 2);
        for pair in bytes.chunks_exact(2) {
            samples.push(i16::from_be_bytes([pair[0], pair[1]]) as f32 / 32768.0);
        }
        self.shared.lock().unwrap().line.push(&samples, self.limit);
    }
    pub fn set_delay_ms(&self, ms: u32) {
        self.delay.set_ms(ms);
    }
    pub fn delay_ms(&self) -> u32 {
        self.delay.ms()
    }
    pub fn dropped_frames(&self) -> u64 {
        self.shared.lock().unwrap().line.dropped_frames
    }
    /// Shared endpoint volume of the local output device.
    pub fn local_volume(&self) -> LocalVolume {
        self.volume.clone()
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(w) = self.worker.take() {
            let _ = w.join();
        }
    }
}
fn render(
    endpoint: Option<String>,
    rate: u32,
    delay: Delay,
    stop: Arc<AtomicBool>,
    shared: &Arc<Mutex<Shared>>,
    volume: &LocalVolume,
    ready: &mpsc::SyncSender<std::result::Result<(), String>>,
) -> Result<()> {
    unsafe { CoInitializeEx(None, COINIT_MULTITHREADED).ok() }?;
    let _com = ComGuard;
    let _priority = crate::wasapi::Mmcss::new();
    let enumerator: IMMDeviceEnumerator =
        unsafe { CoCreateInstance(&MMDeviceEnumerator, None, CLSCTX_ALL) }?;
    let device = crate::wasapi::find_device(&enumerator, endpoint.as_deref())?;
    // The endpoint's own volume control keeps the speakers in step with the Windows mixer.
    let endpoint_volume: Option<IAudioEndpointVolume> =
        unsafe { device.Activate(CLSCTX_ALL, None) }.ok();
    if let Some(control) = &endpoint_volume {
        if let Ok(level) = unsafe { control.GetMasterVolumeLevelScalar() } {
            volume.publish((level * 100.0).round().clamp(0.0, 100.0) as u8);
        }
    }
    let client: IAudioClient = unsafe { device.Activate(CLSCTX_ALL, None) }?;
    let format = WAVEFORMATEX {
        wFormatTag: IEEE_FLOAT,
        nChannels: CHANNELS as u16,
        nSamplesPerSec: rate,
        nAvgBytesPerSec: rate * CHANNELS as u32 * 4,
        nBlockAlign: CHANNELS as u16 * 4,
        wBitsPerSample: 32,
        cbSize: 0,
    };
    let event = EventGuard(unsafe { CreateEventW(None, false, false, None) }?);
    let flags = AUDCLNT_STREAMFLAGS_EVENTCALLBACK
        | AUDCLNT_STREAMFLAGS_NOPERSIST
        | AUTOCONVERT_PCM
        | SRC_DEFAULT_QUALITY;
    unsafe { client.Initialize(AUDCLNT_SHAREMODE_SHARED, flags, 200_000, 0, &format, None) }?;
    unsafe { client.SetEventHandle(event.0) }?;
    let renderer: IAudioRenderClient = unsafe { client.GetService() }?;
    let buffer_frames = unsafe { client.GetBufferSize() }? as usize;
    // The stream already delays audio by its own buffer; count it against the target.
    let hardware_frames = match unsafe { client.GetStreamLatency() } {
        Ok(latency) if latency > 0 => (latency as u64 * rate as u64 / 10_000_000) as usize,
        _ => 0,
    };
    ensure!(buffer_frames > 0, "local monitor reported an empty buffer");
    unsafe { client.Start() }?;
    let _audio = AudioGuard(client.clone());
    let _ = ready.send(Ok(()));
    let tolerance = rate as usize * TRIM_TOLERANCE_MS as usize / 1000;
    let mut out = Vec::with_capacity(buffer_frames * CHANNELS);
    while !stop.load(Ordering::Acquire) {
        if unsafe { WaitForSingleObject(event.0, 50) } != WAIT_OBJECT_0 {
            continue;
        }
        if let (Some(control), Some(percent)) = (&endpoint_volume, volume.pending()) {
            if unsafe { control.SetMasterVolumeLevelScalar(f32::from(percent) / 100.0, std::ptr::null()) }.is_ok() {
                volume.publish(percent);
            }
            volume.settle();
        }
        let padding = unsafe { client.GetCurrentPadding() }? as usize;
        let available = buffer_frames.saturating_sub(padding);
        if available == 0 {
            continue;
        }
        out.clear();
        {
            let mut guard = shared.lock().unwrap();
            let target = delay.frames(rate).saturating_sub(hardware_frames);
            guard.line.trim(target, tolerance);
            guard.line.fill(available, target, &mut out);
        }
        let data = unsafe { renderer.GetBuffer(available as u32) }?;
        let slice = unsafe { std::slice::from_raw_parts_mut(data.cast::<f32>(), available * CHANNELS) };
        for slot in slice.iter_mut() {
            *slot = 0.0;
        }
        for (slot, value) in slice.iter_mut().zip(&out) {
            *slot = *value;
        }
        unsafe { renderer.ReleaseBuffer(available as u32, 0) }?;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn line(samples: &[f32]) -> DelayLine {
        let mut line = DelayLine::default();
        line.push(samples, 44100 * 3 * CHANNELS);
        line
    }
    #[test]
    fn local_volume_reports_unknown_until_the_endpoint_answers() {
        let volume = LocalVolume::default();
        assert_eq!(volume.volume(), None);
        assert!(!volume.available());
        assert_eq!(volume.pending(), None);
        volume.set(140);
        assert_eq!(volume.pending(), Some(100), "host values are clamped to 100");
        volume.settle();
        assert_eq!(volume.pending(), None);
        volume.publish(0);
        assert_eq!(volume.volume(), Some(0), "zero is a real volume, not an unknown one");
        assert!(volume.available());
    }
    #[test]
    fn delay_is_clamped_and_converted_to_frames() {
        let delay = Delay::new(10_000);
        assert_eq!(delay.ms(), MAX_DELAY_MS);
        assert_eq!(delay.frames(44100), MAX_DELAY_MS as usize * 44100 / 1000);
        delay.set_ms(250);
        assert_eq!(delay.frames(48000), 250 * 48000 / 1000);
        assert_eq!(Delay::default().ms(), 0);
    }
    #[test]
    fn output_lags_input_by_the_requested_delay() {
        // Capture and render advance together, one frame per round. With a
        // two-frame target the first two rounds are silent and the audio that
        // follows is the audio captured two rounds earlier.
        let mut line = DelayLine::default();
        let mut out = Vec::new();
        for frame in 1..=6 {
            let value = frame as f32;
            line.push(&[value, value], 44100 * 3 * CHANNELS);
            line.fill(1, 2, &mut out);
        }
        assert_eq!(out, vec![0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0]);
    }
    #[test]
    fn zero_delay_passes_audio_through_and_pads_starvation() {
        let mut line = line(&[1.0, 1.0, 2.0, 2.0]);
        let mut out = Vec::new();
        line.fill(4, 0, &mut out);
        assert_eq!(out, vec![1.0, 1.0, 2.0, 2.0, 0.0, 0.0, 0.0, 0.0]);
    }
    #[test]
    fn starvation_keeps_the_target_delay_after_refill() {
        let mut line = DelayLine::default();
        let limit = 44100 * 3 * CHANNELS;
        let mut out = Vec::new();
        line.push(&[1.0, 1.0, 2.0, 2.0], limit);
        line.fill(2, 2, &mut out);
        assert_eq!(out, vec![0.0, 0.0, 0.0, 0.0]);
        // The capture stalls: the buffered audio is kept and silence is
        // rendered, so the copy does not drift towards the source.
        out.clear();
        line.fill(2, 2, &mut out);
        assert_eq!(out, vec![0.0, 0.0, 0.0, 0.0]);
        // The capture resumes and the audio still comes out two frames late.
        line.push(&[3.0, 3.0, 4.0, 4.0], limit);
        out.clear();
        line.fill(2, 2, &mut out);
        assert_eq!(out, vec![1.0, 1.0, 2.0, 2.0]);
        line.push(&[5.0, 5.0, 6.0, 6.0], limit);
        out.clear();
        line.fill(2, 2, &mut out);
        assert_eq!(out, vec![3.0, 3.0, 4.0, 4.0]);
    }
    #[test]
    fn drift_beyond_the_tolerance_is_trimmed() {
        let mut line = line(&[1.0; 400]);
        line.trim(4, 2);
        assert_eq!(line.samples.len(), (4 + 2) * CHANNELS);
        assert!(line.dropped_frames > 0);
    }
    #[test]
    fn pushed_audio_is_bounded() {
        let mut line = DelayLine::default();
        for _ in 0..10 {
            line.push(&[0.5; 44100 * 2], 44100 * 3 * CHANNELS);
        }
        assert_eq!(line.samples.len(), 44100 * 3 * CHANNELS);
        assert!(line.dropped_frames > 0);
    }
}
