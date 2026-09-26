//! Server CPU and latency with many quiet connections over loopback (Linux only).
//!
//! One server thread runs an event loop over `CONNS` non-blocking connections.
//! One client thread sends small binary messages (64 to 512 bytes) round robin
//! over all connections at a fixed total rate, well below saturation. After each
//! send it tells the server which connection is ready, like epoll would. The
//! server then reads that connection until `WouldBlock`, as an async runtime
//! does, so nearly every wakeup reads one frame and then gets `WouldBlock`.
//!
//! Prints the CPU time of the server thread per received message, from
//! `/proc/thread-self/schedstat`, and the send to receive latency.
//!
//! `cargo bench --bench many_conns`
use rand::{rngs::SmallRng, RngExt, SeedableRng};
use std::{
    fs,
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use tungstenite::{
    protocol::{Role, WebSocketConfig},
    Error, Message, WebSocket,
};

const CONNS: usize = 100;
const MSGS_PER_SEC: u32 = 20_000;
const WARMUP: Duration = Duration::from_secs(1);
const MEASURE: Duration = Duration::from_secs(5);
/// Messages sent after the measured window mark its end for the server.
const COOLDOWN: Duration = Duration::from_millis(100);

fn main() {
    println!(
        "{CONNS} connections, {MSGS_PER_SEC} msg/s total, 64 to 512 B, {}s",
        MEASURE.as_secs()
    );
    for read_buffer_size in [128 * 1024, 64 * 1024] {
        let (cpu, latencies) = run(WebSocketConfig::default().read_buffer_size(read_buffer_size));
        println!(
            "read_buffer_size {:>3} KiB: {:.2} µs server CPU/msg, latency p50 {:>3} µs p99 {:>4} µs ({} msgs)",
            read_buffer_size / 1024,
            cpu.as_nanos() as f64 / latencies.len() as f64 / 1000.0,
            latencies[latencies.len() / 2],
            latencies[latencies.len() * 99 / 100],
            latencies.len(),
        );
    }
}

/// Returns the server CPU time and the sorted latencies in µs of the messages
/// sent in the measured window.
fn run(conf: WebSocketConfig) -> (Duration, Vec<u64>) {
    let epoch = Instant::now();
    let measured = WARMUP.as_nanos() as u64..(WARMUP + MEASURE).as_nanos() as u64;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let mut clients = Vec::new();
    let mut servers = Vec::new();
    for _ in 0..CONNS {
        let client = TcpStream::connect(addr).unwrap();
        client.set_nodelay(true).unwrap();
        clients.push(WebSocket::from_raw_socket(client, Role::Client, None));
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        servers.push(WebSocket::from_raw_socket(server, Role::Server, Some(conf)));
    }

    let (ready_tx, ready_rx) = mpsc::channel();
    let client = thread::spawn(move || send_paced(clients, ready_tx, epoch));

    // The server samples its own CPU time when it receives the first message sent
    // in the measured window and the first one sent after it.
    let cpu_time = || {
        let stat = fs::read_to_string("/proc/thread-self/schedstat").unwrap();
        Duration::from_nanos(stat.split(' ').next().unwrap().parse().unwrap())
    };
    let mut cpu_start = None;
    let mut cpu_end = None;
    let mut latencies = Vec::new();
    let mut open = CONNS;
    while open > 0 {
        let ws: &mut WebSocket<_> = &mut servers[ready_rx.recv().unwrap()];
        loop {
            match ws.read() {
                Ok(Message::Binary(msg)) => {
                    let now = epoch.elapsed().as_nanos() as u64;
                    let sent = u64::from_le_bytes(msg[..8].try_into().unwrap());
                    if measured.contains(&sent) {
                        cpu_start.get_or_insert_with(cpu_time);
                        latencies.push((now - sent) / 1000);
                    } else if sent >= measured.end {
                        cpu_end.get_or_insert_with(cpu_time);
                    }
                }
                Ok(_) => open -= 1,
                Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("{e}"),
            }
        }
    }
    // The clients close only here, after the server read everything.
    drop(client.join().unwrap());
    latencies.sort_unstable();
    (cpu_end.unwrap() - cpu_start.unwrap(), latencies)
}

/// Sends one message every `1 / MSGS_PER_SEC` seconds, round robin over all
/// connections, until `COOLDOWN` after the measured window, and then a text
/// message on each to stop. Each binary message starts with its send time in ns
/// since `epoch`. Returns the clients, so the caller decides when they close.
fn send_paced(
    mut clients: Vec<WebSocket<TcpStream>>,
    ready: mpsc::Sender<usize>,
    epoch: Instant,
) -> Vec<WebSocket<TcpStream>> {
    let mut rng = SmallRng::seed_from_u64(123);
    let interval = Duration::from_secs(1) / MSGS_PER_SEC;
    let mut next = Duration::ZERO;
    let mut n = 0;
    while next < WARMUP + MEASURE + COOLDOWN {
        while next <= epoch.elapsed() {
            let mut msg = vec![0; rng.random_range(64..=512)];
            msg[..8].copy_from_slice(&(epoch.elapsed().as_nanos() as u64).to_le_bytes());
            clients[n % CONNS].send(Message::binary(msg)).unwrap();
            ready.send(n % CONNS).unwrap();
            n += 1;
            next += interval;
        }
        thread::sleep(next.saturating_sub(epoch.elapsed()));
    }
    for (n, client) in clients.iter_mut().enumerate() {
        client.send(Message::text("stop")).unwrap();
        ready.send(n).unwrap();
    }
    clients
}
