//! Device volume is receiver state, independent of PCM gain. All RTSP I/O stays
//! off the media thread and shares the feedback connection's transaction lock.
use crate::rtsp::{Cancellation, Connection};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Commands without a device target apply to every device in the session.
pub const ALL_DEVICES: &str = "*";

#[derive(Clone, Debug)]
pub struct Command {
    pub sequence: u64,
    pub percent: u8,
    /// None applies to every device in the session; Some targets one device.
    pub host: Option<String>,
}
#[derive(Clone, Default)]
pub struct Control(Arc<Mutex<HashMap<String, Command>>>);
impl Control {
    pub fn set(&self, command: Command) {
        let key = command.host.clone().unwrap_or_else(|| ALL_DEVICES.to_string());
        let mut latest = self.0.lock().unwrap();
        if latest.get(&key).is_none_or(|old| command.sequence > old.sequence) {
            latest.insert(key, command);
        }
    }
    fn latest(&self) -> HashMap<String, Command> {
        self.0.lock().unwrap().clone()
    }
}
pub type Peer = (String, String, Arc<Mutex<Connection>>);
pub fn to_db(percent: u8) -> f64 {
    if percent == 0 {
        -144.0
    } else {
        f64::from(percent) * 0.3 - 30.0
    }
}
pub fn from_db(db: f64) -> Result<u8> {
    ensure!(db.is_finite() && db <= 0.0, "invalid receiver volume");
    Ok(if db <= -30.0 {
        0
    } else {
        ((db + 30.0) / 0.3).round() as u8
    })
}
fn read(connection: &mut Connection) -> Result<u8> {
    let info = connection.request("GET", "/info", &[], &[])?.plist()?;
    let value = info
        .as_dictionary()
        .and_then(|d| d.get("initialVolume"))
        .context("receiver does not report initialVolume")?;
    let db = value
        .as_real()
        .or_else(|| value.as_signed_integer().map(|n| n as f64))
        .context("invalid initialVolume type")?;
    from_db(db)
}
#[derive(Default, Clone)]
struct Pending {
    command: Option<Command>,
    sent: Option<Instant>,
    write_failed: bool,
    settled: bool,
}
impl Pending {
    /// A session-wide command needs every device to reach the requested value.
    fn evaluate_all(&mut self, values: &[Option<u8>], now: Instant) -> &'static str {
        let readable = values.iter().all(Option::is_some);
        if let Some(command) = &self.command {
            if !self.settled {
                if !self.write_failed
                    && readable
                    && values.iter().all(|v| *v == Some(command.percent))
                {
                    self.settled = true;
                    return "confirmed";
                }
                if !self.write_failed
                    && self
                        .sent
                        .is_some_and(|at| now.duration_since(at) < Duration::from_secs(3))
                {
                    return "pending";
                }
                self.settled = true;
                return "unconfirmed";
            }
        }
        if readable { "confirmed" } else { "unsynced" }
    }
    /// A targeted command only waits for the one device it addressed.
    fn evaluate_one(&mut self, value: Option<u8>, now: Instant) -> &'static str {
        if let Some(command) = &self.command {
            if !self.settled {
                if !self.write_failed && value == Some(command.percent) {
                    self.settled = true;
                    return "confirmed";
                }
                if !self.write_failed
                    && self
                        .sent
                        .is_some_and(|at| now.duration_since(at) < Duration::from_secs(3))
                {
                    return "pending";
                }
                self.settled = true;
                return "unconfirmed";
            }
        }
        if value.is_some() { "confirmed" } else { "unsynced" }
    }
}
/// Report one device against its own command first, then a session-wide one.
fn member_status(
    peers: &[Peer],
    index: usize,
    values: &[Option<u8>],
    pending: &mut HashMap<String, Pending>,
    now: Instant,
) -> String {
    let host = &peers[index].0;
    let own = pending.get(host).and_then(|p| p.command.as_ref()).map(|c| c.sequence);
    let all = pending.get(ALL_DEVICES).and_then(|p| p.command.as_ref()).map(|c| c.sequence);
    if own.is_some_and(|sequence| all.is_none_or(|latest| sequence > latest)) {
        return pending
            .get_mut(host)
            .expect("targeted pending entry")
            .evaluate_one(values[index], now)
            .to_string();
    }
    if all.is_some() {
        // A device with a newer targeted command waits for that command alone.
        let latest = all.unwrap_or_default();
        let scoped: Vec<Option<u8>> = peers
            .iter()
            .enumerate()
            .filter(|&(position, (host, _, _))| {
                position == index
                    || !pending
                        .get(host)
                        .and_then(|p| p.command.as_ref())
                        .is_some_and(|c| c.sequence > latest)
            })
            .map(|(position, _)| values[position])
            .collect();
        return pending
            .get_mut(ALL_DEVICES)
            .expect("session-wide pending entry")
            .evaluate_all(&scoped, now)
            .to_string();
    }
    if values[index].is_some() { "confirmed".into() } else { "unsynced".into() }
}
pub struct Worker {
    stop: Cancellation,
    worker: Option<JoinHandle<()>>,
    pub events: mpsc::Receiver<Value>,
}
impl Worker {
    /// Peers are ordered with the stereo leader first by the desktop controller.
    pub fn start(peers: Vec<Peer>, control: Control) -> Result<Self> {
        ensure!(!peers.is_empty(), "volume worker requires a receiver");
        let stop = Cancellation::default();
        let done = stop.clone();
        let (tx, events) = mpsc::sync_channel(32);
        let worker = thread::Builder::new().name("airplay-volume".into()).spawn(move || {
            let mut pending: HashMap<String, Pending> = HashMap::new();
            let mut next = Instant::now();
            while !done.is_cancelled() {
                let commands = control.latest();
                let mut errors = Vec::new();
                let mut wrote = false;
                for (key, command) in &commands {
                    let stale = pending
                        .get(key)
                        .is_some_and(|p| p.command.as_ref().is_some_and(|c| c.sequence >= command.sequence));
                    if stale {
                        continue;
                    }
                    let entry = pending.entry(key.clone()).or_default();
                    *entry = Pending { command: Some(command.clone()), sent: Some(Instant::now()), ..Pending::default() };
                    // Each device keeps its newest command: a session-wide write skips
                    // devices with a newer targeted one, and a targeted write yields to a
                    // newer session-wide one. Order inside a cycle cannot matter then.
                    let targets: Vec<&Peer> = match &command.host {
                        Some(host) if commands.get(ALL_DEVICES).is_some_and(|wide| wide.sequence > command.sequence) => Vec::new(),
                        Some(host) => peers.iter().filter(|(h, _, _)| h == host).collect(),
                        None => peers.iter().filter(|(host, _, _)| {
                            !commands.get(host.as_str()).is_some_and(|own| own.sequence > command.sequence)
                        }).collect(),
                    };
                    for (host, uri, connection) in targets {
                        if done.is_cancelled() { return; }
                        let result = connection.lock().unwrap().request("SET_PARAMETER", uri,
                            &[("Content-Type", "text/parameters".into())],
                            format!("volume: {:.6}\r\n", to_db(command.percent)).as_bytes());
                        if let Err(error) = result {
                            entry.write_failed = true;
                            errors.push(format!("{host}: {error:#}"));
                        }
                    }
                    wrote = true;
                }
                if !wrote && Instant::now() < next {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }
                let mut values = Vec::new();
                for (host, _, connection) in &peers {
                    if done.is_cancelled() { return; }
                    match read(&mut connection.lock().unwrap()) {
                        Ok(value) => values.push(Some(value)),
                        Err(error) => { values.push(None); errors.push(format!("{host}: {error:#}")); }
                    }
                }
                let now = Instant::now();
                let members: Vec<Value> = peers.iter().zip(&values).enumerate().map(|(index, ((host, _, _), value))| {
                    json!({"host":host,"volume":value,"status":member_status(&peers, index, &values, &mut pending, now)})
                }).collect();
                let _ = tx.try_send(json!({"event":"device_volume","host":peers[0].0,
                    "sequence":pending.values().filter_map(|p| p.command.clone()).map(|c| c.sequence).max().unwrap_or(0),
                    "volume":values[0],"available":values.iter().all(Option::is_some),
                    "status":members[0]["status"],"message":errors.join("; "),"members":members}));
                next = Instant::now() + Duration::from_secs(1);
            }
        })?;
        Ok(Self {
            stop,
            worker: Some(worker),
            events,
        })
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn conversion_and_invalid_values() {
        for percent in 0..=100 {
            assert_eq!(from_db(to_db(percent)).unwrap(), percent);
        }
        assert_eq!(from_db(-144.0).unwrap(), 0);
        assert!(from_db(f64::NAN).is_err());
        assert!(from_db(f64::INFINITY).is_err());
        assert!(from_db(1.0).is_err());
    }
    #[test]
    fn delayed_reads_and_pair_confirmation() {
        let now = Instant::now();
        let mut pending = Pending {
            command: Some(Command {
                sequence: 1,
                percent: 40,
                host: None,
            }),
            sent: Some(now),
            ..Pending::default()
        };
        assert_eq!(pending.evaluate_all(&[Some(20), Some(20)], now), "pending");
        assert_eq!(pending.evaluate_all(&[Some(40), Some(20)], now), "pending");
        assert_eq!(pending.evaluate_all(&[Some(40), Some(40)], now), "confirmed");
        // External physical changes are authoritative once the write is confirmed.
        assert_eq!(pending.evaluate_all(&[Some(45), Some(45)], now), "confirmed");
    }
    #[test]
    fn timeout_and_missing_reads_do_not_confirm() {
        let now = Instant::now();
        let mut pending = Pending {
            command: Some(Command {
                sequence: 1,
                percent: 40,
                host: None,
            }),
            sent: Some(now),
            ..Pending::default()
        };
        assert_eq!(pending.evaluate_all(&[None], now), "pending");
        assert_eq!(
            pending.evaluate_all(&[Some(20)], now + Duration::from_secs(3)),
            "unconfirmed"
        );
        assert_eq!(
            pending.evaluate_all(&[None], now + Duration::from_secs(4)),
            "unsynced"
        );
    }
    #[test]
    fn targeted_command_confirms_its_own_device_only() {
        let now = Instant::now();
        let mut targeted = Pending {
            command: Some(Command {
                sequence: 1,
                percent: 40,
                host: Some("192.0.2.10".into()),
            }),
            sent: Some(now),
            ..Pending::default()
        };
        // The other device keeps its old value; the targeted one settles alone.
        assert_eq!(targeted.evaluate_one(Some(20), now), "pending");
        assert_eq!(targeted.evaluate_one(Some(40), now), "confirmed");
        // A targeted write that never reaches its device times out alone.
        let mut late = Pending {
            command: Some(Command {
                sequence: 2,
                percent: 40,
                host: Some("192.0.2.10".into()),
            }),
            sent: Some(now),
            ..Pending::default()
        };
        assert_eq!(late.evaluate_one(Some(20), now + Duration::from_secs(4)), "unconfirmed");
    }
    #[test]
    fn control_tracks_each_device_separately() {
        let control = Control::default();
        control.set(Command { sequence: 2, percent: 30, host: Some("192.0.2.10".into()) });
        control.set(Command { sequence: 1, percent: 80, host: Some("192.0.2.10".into()) });
        control.set(Command { sequence: 3, percent: 55, host: None });
        let latest = control.latest();
        assert_eq!(latest.len(), 2);
        assert_eq!(latest.get("192.0.2.10").unwrap().percent, 30);
        assert_eq!(latest.get(ALL_DEVICES).unwrap().percent, 55);
    }
    #[test]
    fn latest_command_wins() {
        let control = Control::default();
        control.set(Command {
            sequence: 2,
            percent: 30,
            host: None,
        });
        control.set(Command {
            sequence: 1,
            percent: 80,
            host: None,
        });
        assert_eq!(control.latest().get(ALL_DEVICES).unwrap().percent, 30);
    }
}
