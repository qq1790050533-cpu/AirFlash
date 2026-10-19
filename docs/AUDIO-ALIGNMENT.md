# Audio alignment

AirPlay receivers render later than the Windows source: the local device plays the
captured audio almost immediately, while the receiver adds its negotiated buffer,
network jitter handling, and its own output path. **Audio alignment** renders the
same audio on a local output device after a delay you choose, so the local
speakers and the receiver become audible at the same time.

## How it works

```
Windows audio -> capture endpoint -> engine -> AirPlay receiver (delayed by the receiver)
                                          \-> local monitor: delay line -> local playback device
```

1. The capture endpoint feeds the engine exactly as before.
2. Every packet sent to the receivers is also buffered by the local monitor.
3. The monitor renders that audio to a local output device, held back by the
   configured delay. Stream latency reported by WASAPI is subtracted from the
   target, so the slider approximates the acoustic offset rather than the sum of
   the delay and the device buffer.

The delay is applied inside the engine and can be changed while a session is
running. Moving the slider in **Settings > Audio capture > Local alignment**
sends `set_local_monitor` to the engine; no reconnection is required. The value
also survives restarts when you click Apply.

## Constraints

- **The local playback device must differ from the capture endpoint.** Otherwise
  the dry source and the delayed copy overlap, and mute-while-streaming (which
  mutes at the endpoint level) would silence the monitor too. The engine rejects
  a start request where both resolve to the same endpoint, and Settings blocks
  the same combination before it is saved.
- **Alignment is a manual, acoustic adjustment.** AirFlash does not measure
  end-to-end acoustic latency; it delays one path by the requested number of
  milliseconds. Start from the stream latency mode in Settings (real-time targets
  120 ms, normal 200 ms, buffered 500 ms) and add the receiver's own output delay.
- The monitor follows master volume and mute, because it renders the audio that
  was sent to the receivers.

## Recommended setups

| Goal | Capture | Local playback device |
|---|---|---|
| HomePod plus PC speakers | Recording endpoint (Voicemeeter bus, VB-Cable output) | PC speakers (default render device) |
| HomePod plus a second output (USB DAC, Bluetooth) | Default system output | The second output device |
| HomePod only | Any | Leave alignment disabled |

For the first row, route application audio into Voicemeeter or a virtual cable,
capture its recording endpoint, and let AirFlash render the delayed copy to the
speakers. Nothing plays the dry signal locally, so no mute is required.

For the second row, keep **Mute local output while streaming** enabled so the
captured device stays silent while the monitor plays the delayed copy.

## Tuning

1. Start playback and pick a track with clear transients (percussive hits).
2. Set the delay to 0 ms and increase it slowly.
3. Stop where the local speakers and the receiver sound like one source instead
   of an echo. Typical values are 150–500 ms, depending on the latency mode and
   the receiver.

Engine events `local_monitor` and `local_monitor_changed` report the device and
delay in use; both appear in the log folder when diagnostics are enabled.

## Automatic delay detection

The receivers already negotiate an output latency during `SETUP` and report it in
`latencyMin`. Enable **Detect receiver delay automatically** and the engine takes
the slowest value the receivers report as the local alignment delay, so the local
copy waits exactly as long as the weakest AirPlay path. The slider is disabled
while detection is on; the manual value is still kept in settings.

The chosen value is reported through the `alignment_auto` event (fields
`delay_ms`, `member_latency_ms`, `requested_latency_ms`) and shown on the
**Monitor** page as *Detected receiver latency*. Detection runs once per session
when the local monitor starts; re-enabling the setting restarts the session.

Automatic detection removes the guesswork of the *target* delay, but it cannot
measure the receiver's acoustic path. If the room still sounds like an echo, nudge
the manual slider on top of the detected value.

## Channel split: which device is left, which is right

Every output — each AirPlay receiver and, optionally, the local playback device —
can play **both channels** (default), **left only**, or **right only**. A split
turns two devices into one stereo pair: one left, one right.

Rules, enforced by the engine and by Settings before the session starts:

- A split must cover **every** output of the session: no output may keep playing
  both channels.
- A split needs **exactly one left and one right** output. Two single devices, or
  one receiver plus the local playback device, work; three split outputs do not.
- Changing any output's channel restarts the session (the delay does not).
- If a split is rejected, the session stops with the rule it broke; saving a repaired
  layout retries the same receiver automatically — no manual stop/start needed.

Set the channel of each physical device under **Settings > Receivers > Advanced
settings > Channel** (a stereo pair lists one row per member) and the channel of
the local copy under **Settings > Audio capture > Local alignment > Channel**.
