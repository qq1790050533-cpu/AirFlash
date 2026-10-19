//! Real localhost RTSP transactions. No speaker or audio device is opened.
use airflash_engine::{
    rtsp::{Cancellation, Connection},
    volume::{Command, Control, Worker},
};
use plist::Value;
use std::{
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

type Fake = (
    airflash_engine::volume::Peer,
    Arc<AtomicU8>,
    Arc<AtomicUsize>,
    Cancellation,
    thread::JoinHandle<()>,
);
fn fake(host: &str, initial: u8, reject: bool) -> Fake {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let connection =
        Connection::connect(listener.local_addr().unwrap(), Cancellation::default()).unwrap();
    let (socket, _) = listener.accept().unwrap();
    let value = Arc::new(AtomicU8::new(initial));
    let sets = Arc::new(AtomicUsize::new(0));
    let stop = Cancellation::default();
    let (v, n, done) = (value.clone(), sets.clone(), stop.clone());
    let server = thread::spawn(move || {
        let mut c = Connection::from_stream(socket, done.clone()).unwrap();
        let mut delayed = None;
        while !done.is_cancelled() {
            let msg = match c.read_for(Duration::from_millis(20), None) {
                Ok(Some(m)) => m,
                Ok(None) => continue,
                Err(_) => break,
            };
            let mut body = Vec::new();
            if msg.first.starts_with("GET /info ") {
                let mut info = plist::Dictionary::new();
                if !reject {
                    info.insert(
                        "initialVolume".into(),
                        Value::Real(airflash_engine::volume::to_db(v.load(Ordering::Relaxed))),
                    );
                }
                Value::Dictionary(info).to_writer_binary(&mut body).unwrap();
                // First read after SET still returns the old value.
                if let Some(next) = delayed.take() {
                    v.store(next, Ordering::Relaxed);
                }
            } else if msg.first.starts_with("SET_PARAMETER ") {
                assert_eq!(msg.headers["content-type"], "text/parameters");
                let text = String::from_utf8(msg.body).unwrap();
                let db: f64 = text
                    .trim()
                    .strip_prefix("volume: ")
                    .unwrap()
                    .parse()
                    .unwrap();
                delayed = Some(airflash_engine::volume::from_db(db).unwrap());
                n.fetch_add(1, Ordering::Relaxed);
            } else {
                panic!("unexpected transaction: {}", msg.first);
            }
            let mut response = format!(
                "RTSP/1.0 200 OK\r\nCSeq: {}\r\nContent-Length: {}\r\n\r\n",
                msg.headers["cseq"],
                body.len()
            )
            .into_bytes();
            response.extend(body);
            if c.write(&response).is_err() {
                break;
            }
        }
    });
    (
        (
            host.to_string(),
            format!("rtsp://{host}/test"),
            Arc::new(Mutex::new(connection)),
        ),
        value,
        sets,
        stop,
        server,
    )
}
#[test]
fn initial_read_physical_change_pair_write_and_delayed_confirmation() {
    let (leader, volume, sets1, stop1, server1) = fake("127.0.0.1", 28, false);
    let (member, _, sets2, stop2, server2) = fake("127.0.0.1", 25, false);
    let control = Control::default();
    let worker = Worker::start(vec![leader, member], control.clone()).unwrap();
    let initial = worker.events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(initial["volume"], 28);
    assert_eq!(sets1.load(Ordering::Relaxed), 0);
    assert_eq!(sets2.load(Ordering::Relaxed), 0);
    volume.store(32, Ordering::Relaxed);
    let external = worker.events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(external["volume"], 32);
    control.set(Command {
        sequence: 1,
        percent: 40,
        host: None,
    });
    let old = worker.events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(old["sequence"], 1);
    assert_eq!(old["status"], "pending");
    assert_eq!(old["volume"], 32);
    let confirmed = worker.events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(confirmed["status"], "confirmed");
    assert_eq!(confirmed["volume"], 40);
    assert_eq!(sets1.load(Ordering::Relaxed), 1);
    assert_eq!(sets2.load(Ordering::Relaxed), 1);
    stop1.cancel();
    stop2.cancel();
    drop(worker);
    server1.join().unwrap();
    server2.join().unwrap();
}
#[test]
fn a_newer_session_wide_command_supersedes_a_targeted_one() {
    let (leader, volume1, sets1, stop1, server1) = fake("127.0.0.1", 28, false);
    let (member, _, sets2, stop2, server2) = fake("127.0.0.2", 25, false);
    let control = Control::default();
    let worker = Worker::start(vec![leader, member], control.clone()).unwrap();
    let initial = worker.events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(initial["members"][0]["volume"], 28);
    assert_eq!(initial["members"][1]["volume"], 25);
    control.set(Command { sequence: 1, percent: 60, host: Some("127.0.0.2".into()) });
    let pending = loop {
        let event = worker.events.recv_timeout(Duration::from_secs(2)).unwrap();
        if event["members"][1]["status"] == "pending" { break event; }
    };
    assert_eq!(pending["members"][0]["status"], "confirmed");
    control.set(Command { sequence: 2, percent: 40, host: None });
    let deadline = Instant::now() + Duration::from_secs(6);
    let confirmed = loop {
        let event = worker
            .events
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap();
        if event["members"][0]["status"] == "confirmed" && event["members"][1]["status"] == "confirmed" {
            break event;
        }
    };
    // Both devices settle on the newer session-wide value; neither reports a timeout.
    assert_eq!(confirmed["members"][0]["volume"], 40);
    assert_eq!(confirmed["members"][1]["volume"], 40);
    assert_eq!(volume1.load(Ordering::Relaxed), 40);
    assert_eq!(sets1.load(Ordering::Relaxed), 1);
    assert_eq!(sets2.load(Ordering::Relaxed), 2);
    stop1.cancel();
    stop2.cancel();
    drop(worker);
    server1.join().unwrap();
    server2.join().unwrap();
}
#[test]
fn unsupported_query_does_not_invent_volume() {
    let (peer, _, sets, stop, server) = fake("127.0.0.1", 28, true);
    let worker = Worker::start(vec![peer], Control::default()).unwrap();
    let event = worker.events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(event["volume"].is_null());
    assert_eq!(event["available"], false);
    assert_eq!(event["status"], "unsynced");
    assert_eq!(sets.load(Ordering::Relaxed), 0);
    stop.cancel();
    drop(worker);
    server.join().unwrap();
}
#[test]
fn targeted_command_writes_only_its_device() {
    let (leader, volume, sets1, stop1, server1) = fake("127.0.0.1", 28, false);
    let (member, _, sets2, stop2, server2) = fake("127.0.0.2", 25, false);
    let control = Control::default();
    let worker = Worker::start(vec![leader, member], control.clone()).unwrap();
    let initial = worker.events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(initial["members"][0]["volume"], 28);
    assert_eq!(initial["members"][1]["volume"], 25);
    volume.store(32, Ordering::Relaxed);
    let external = worker.events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(external["members"][0]["volume"], 32);
    control.set(Command { sequence: 1, percent: 60, host: Some("127.0.0.2".into()) });
    let pending = worker.events.recv_timeout(Duration::from_secs(2)).unwrap();
    // The leader keeps its own value and stays confirmed; only the member waits.
    assert_eq!(pending["members"][0]["status"], "confirmed");
    assert_eq!(pending["members"][1]["status"], "pending");
    assert_eq!(pending["status"], "confirmed");
    let deadline = Instant::now() + Duration::from_secs(6);
    let confirmed = loop {
        let event = worker
            .events
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap();
        if event["members"][1]["status"] == "confirmed" {
            break event;
        }
    };
    assert_eq!(confirmed["members"][1]["volume"], 60);
    assert_eq!(confirmed["members"][0]["volume"], 32);
    assert_eq!(sets1.load(Ordering::Relaxed), 0);
    assert_eq!(sets2.load(Ordering::Relaxed), 1);
    stop1.cancel();
    stop2.cancel();
    drop(worker);
    server1.join().unwrap();
    server2.join().unwrap();
}
