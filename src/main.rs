//! N1MM+ peer protocol bridge — port 12070
//!
//! Implements the N1MM+ LAN networking protocol so that fd_logger appears as
//! a peer to other N1MM+ stations on the local network.
//!
//! Protocol summary (all on port 12070):
//!   UDP broadcast every ~20s  — announce presence to the subnet
//!   TCP (listener + initiator) — framed DATA__00…~__DATA messages
//!
//! Message frame format:
//!   DATA__00%<sender>%<command>%<fields...>%~__DATA

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use chrono::{Local, Utc};
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio::time::interval;

// Track IPs we currently have an outbound TCP connection to (connect once).
type ConnectedPeers = Arc<TokioMutex<HashSet<IpAddr>>>;

// Map peer SocketAddr → channel sender for pushing outbound TCP messages.
// Used by run_db_watcher to send QSO messages to all connected N1MM peers.
type PeerMap = Arc<TokioMutex<HashMap<SocketAddr, mpsc::Sender<String>>>>;

// ── Version we advertise to N1MM peers ───────────────────────────────────────

const N1MM_VERSION: &str = "1.0.11229.0";
const TCP_PORT: u16 = 12070;
const UDP_PORT: u16 = 12070;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug, Clone)]
#[command(name = "n1mm_bridge", about = "N1MM+ peer protocol bridge for FD Logger")]
struct Args {
    /// Station callsign (e.g. KB3GTN)
    #[arg(long, short = 'c')]
    callsign: String,

    /// Station name / NetBIOS name advertised to N1MM peers (must match Samba NetBIOS name)
    #[arg(long, short = 's', default_value = "FDLOGGER")]
    station: String,

    /// Contest name to advertise (e.g. FD)
    #[arg(long, default_value = "FD")]
    contest: String,

    /// Local IP address to advertise in UDP announces (e.g. 192.168.1.100)
    #[arg(long, short = 'i')]
    local_ip: String,

    /// UDP broadcast address for announces
    #[arg(long, default_value = "255.255.255.255")]
    broadcast: String,

    /// Path to fd_logger.db (enables port 12060 XML contact listener)
    #[arg(long, short = 'd', default_value = "fd_logger.db")]
    db: String,
}

// ── Shared config ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Config {
    callsign:  String,
    station:   String,
    contest:   String,
    local_ip:  String,
    broadcast: String,
    db_path:   String,
}

impl Config {
    fn from_args(a: &Args) -> Self {
        Config {
            callsign:  a.callsign.to_uppercase(),
            station:   a.station.to_uppercase(),
            contest:   a.contest.to_uppercase(),
            local_ip:  a.local_ip.clone(),
            broadcast: a.broadcast.clone(),
            db_path:   a.db.clone(),
        }
    }
}

// ── Message builders ──────────────────────────────────────────────────────────

fn now_parts() -> (String, String) {
    let now = Local::now();
    (now.format("%Y-%m-%d").to_string(), now.format("%H:%M:%S").to_string())
}

/// UDP announce packet: <station>%<ip>%<port>%<version>%<station>%%
fn udp_announce(cfg: &Config) -> String {
    format!("{}%{}%{}%{}%{}%%",
        cfg.station, cfg.local_ip, TCP_PORT, N1MM_VERSION, cfg.station)
}

fn msg_lastqat(cfg: &Config) -> String {
    // TODO: read actual last QSO time from fd_logger.db
    let now = Local::now();
    let ts = now.format("%Y-%m-%d %H:%M:%S").to_string();
    format!("DATA__00%{}%LASTQAT%{}%~__DATA", cfg.station, ts)
}

fn msg_echoreq(cfg: &Config) -> String {
    let (date, time) = now_parts();
    format!("DATA__00%{}%ECHOREQ%{}%{}%~__DATA", cfg.station, date, time)
}

fn msg_echo(cfg: &Config, date: &str, time: &str) -> String {
    format!("DATA__00%{}%ECHO%{}%{}%~__DATA", cfg.station, date, time)
}

fn msg_contestname(cfg: &Config) -> String {
    format!("DATA__00%{}%CONTESTNAME%{}%{}%%~__DATA",
        cfg.station, cfg.callsign, cfg.contest)
}

fn msg_status(cfg: &Config) -> String {
    // rxfreq/txfreq in kHz×100 (1422500 = 14225 kHz = 20m)
    format!("DATA__00%{}%STATUS%1422500%1422500%0%{}%0%0%SB%MULTI-OP%UNLIMITED%ZB%{}%0%0%0%0%0%0%~__DATA",
        cfg.station, cfg.callsign, N1MM_VERSION)
}

fn msg_qsonrs(cfg: &Config) -> String {
    // N1MM's parser reads indices 0-32 (33 elements). The %~__DATA terminator must land
    // at index 33 to avoid being parsed as a band count. With FD%0%0% prefix + 30 zeros,
    // total data fields = 33, putting ~ at index 33 which N1MM never accesses.
    // TODO: read actual counts from fd_logger.db
    let zeros = vec!["0"; 30].join("%");
    format!("DATA__00%{}%QSONRS%{}%0%0%{}%~__DATA",
        cfg.station, cfg.contest, zeros)
}

// ── Outbound contact helpers ──────────────────────────────────────────────────

/// Band lower-edge MHz string matching what real N1MM puts in QSO field[24]
/// and the XML <band> element.
fn band_mhz_str(b: &str) -> &str {
    match b {
        "160M" => "1.8",   "80M"  => "3.5",  "60M"  => "5.3",
        "40M"  => "7.0",   "30M"  => "10.1", "20M"  => "14.0",
        "17M"  => "18.1",  "15M"  => "21.0", "12M"  => "24.9",
        "10M"  => "28.0",  "6M"   => "50.0", "2M"   => "144.0",
        "70CM" => "420.0", _      => "14.0",
    }
}

/// Map fd_logger mode (PH/CW/DIG) to an N1MM mode string.
fn mode_to_n1mm(m: &str) -> &str {
    match m { "CW" => "CW", "DIG" => "DIG", _ => "USB" }
}

/// Build a TCP QSO message replicating the format N1MM stations send each other.
/// field[10] = our station callsign (the "sent exchange" call in N1MM).
fn msg_qso_contact(cfg: &Config, c: &DbContact, n1mm_id: &str) -> String {
    let freq_khz = format!("{:.2}", band_center_khz(&c.band));
    let band_mhz = band_mhz_str(&c.band).to_string();
    let ts       = format!("{} {}:00", c.date, c.time);
    let mode_n   = mode_to_n1mm(&c.mode);
    let prefix: String = c.call.chars().take_while(|ch| ch.is_ascii_alphabetic()).collect();

    // 45 fields (indices 0-44) matching the N1MM QSO wire format observed in captures.
    let fields = [
        "1900-01-01 00:00:00".to_string(), // [0]  legacy base timestamp
        ts,                                 // [1]  QSO timestamp
        c.call.clone(),                     // [2]  contacted call
        freq_khz.clone(),                   // [3]  rx freq kHz
        freq_khz,                           // [4]  tx freq kHz
        mode_n.to_string(),                 // [5]  mode
        cfg.contest.clone(),                // [6]  contest
        "59".into(), "59".into(),           // [7,8] snt/rcv
        "K".into(),                         // [9]  country prefix
        cfg.callsign.clone(),               // [10] our station callsign
        String::new(), String::new(), String::new(), // [11-13]
        "0".into(),                         // [14]
        c.section.clone(),                  // [15] received section
        String::new(), "0".into(),          // [16,17]
        "1".into(),                         // [18] points
        c.id.to_string(),                   // [19] QSO number (db id)
        "1".into(), "0".into(), "0".into(), // [20-22]
        String::new(),                      // [23]
        band_mhz,                           // [24] band MHz
        prefix,                             // [25] call prefix
        c.class.clone(),                    // [26] received class/exchange
        "1".into(),                         // [27]
        c.operator.clone(),                 // [28] operator
        String::new(),                      // [29]
        "1".into(), "0".into(),             // [30,31]
        String::new(),                      // [32]
        "NA".into(),                        // [33] continent
        String::new(), "1".into(),          // [34,35]
        String::new(),                      // [36]
        "0".into(), "0".into(),             // [37,38]
        cfg.station.clone(),                // [39] station name
        "1".into(), "0".into(),             // [40,41]
        n1mm_id.to_string(),               // [42] N1MM GUID (32 hex chars)
        "1".into(), String::new(),          // [43,44]
    ];
    format!("DATA__00%{}%QSO%{}%~__DATA", cfg.station, fields.join("%"))
}

/// Parse a TCP QSO field list into an XmlContact for DB insertion.
fn parse_tcp_qso(fields: &[String]) -> Option<XmlContact> {
    if fields.len() < 43 { return None; }
    let call    = fields[2].trim().to_uppercase();
    let n1mm_id = fields[42].trim().to_string();
    if call.is_empty() || n1mm_id.is_empty() { return None; }

    let freq_khz: f64 = fields[3].trim().parse().unwrap_or(0.0);
    let band = band_from_khz(freq_khz as u32)?;
    let mode = mode_from_n1mm(fields[5].trim());

    let ts = fields[1].trim();
    let (date, time) = match ts.split_once(' ') {
        Some((d, t)) => (d.to_string(), t.get(..5).unwrap_or(t).to_string()),
        None         => (ts.to_string(), "00:00".to_string()),
    };

    Some(XmlContact {
        call,
        band,
        mode,
        class:    fields[26].trim().to_uppercase(),
        section:  fields[15].trim().to_uppercase(),
        operator: fields[28].trim().to_uppercase(),
        date,
        time,
        n1mm_id,
    })
}

// ── Stateful frame reader ─────────────────────────────────────────────────────
//
// N1MM sends multiple messages in a single TCP segment (e.g. ECHOREQ +
// CONTESTNAME + STATUS as one write).  A stateless reader would concatenate
// them and only parse the first.  FrameReader keeps leftover bytes between
// calls so each message is returned individually.

struct FrameReader {
    buf: Vec<u8>,
}

impl FrameReader {
    fn new() -> Self {
        FrameReader { buf: Vec::with_capacity(512) }
    }

    async fn next_frame(&mut self, reader: &mut OwnedReadHalf) -> io::Result<Option<String>> {
        const END: &[u8] = b"~__DATA";
        loop {
            // Return the next complete message from the buffer if one is ready
            if let Some(pos) = self.buf.windows(END.len()).position(|w| w == END) {
                let end = pos + END.len();
                let frame = String::from_utf8_lossy(&self.buf[..end]).to_string();
                self.buf.drain(..end);
                return Ok(Some(frame));
            }

            // Need more data from the network
            let mut tmp = [0u8; 1024];
            let n = reader.read(&mut tmp).await?;
            if n == 0 {
                return Ok(None); // peer closed connection
            }
            self.buf.extend_from_slice(&tmp[..n]);

            if self.buf.len() > 65_536 {
                return Err(io::Error::new(io::ErrorKind::Other, "buffer overflow"));
            }
        }
    }
}

// ── Parsed incoming message ───────────────────────────────────────────────────

#[derive(Debug)]
struct InMsg {
    sender:  String,
    command: String,
    fields:  Vec<String>,
}

/// Parse DATA__00%sender%command%fields%~__DATA into an InMsg.
fn parse_frame(raw: &str) -> Option<InMsg> {
    let raw = raw.trim();
    if !raw.starts_with("DATA__00") || !raw.ends_with("~__DATA") {
        return None;
    }
    let inner = &raw["DATA__00".len()..raw.len() - "~__DATA".len()];
    let parts: Vec<&str> = inner.split('%').collect();
    if parts.len() < 3 {
        return None;
    }
    Some(InMsg {
        sender:  parts[1].to_string(),
        command: parts[2].to_string(),
        fields:  parts[3..].iter().map(|s| s.to_string()).collect(),
    })
}

// ── TCP connection handler ────────────────────────────────────────────────────

async fn handle_connection(
    stream:         TcpStream,
    peer:           SocketAddr,
    cfg:            Arc<Config>,
    outbound_peers: Option<ConnectedPeers>,
    peer_map:       PeerMap,
) {
    println!("[12070] connected: {}", peer);

    let (mut read_half, mut write_half) = stream.into_split();

    // Real N1MM sends only QSONRS + LASTQAT immediately on connect.
    // ECHOREQ/CONTESTNAME/STATUS go out with the first periodic tick (~10s).
    let handshake = [msg_qsonrs(&cfg), msg_lastqat(&cfg)];
    for msg in &handshake {
        println!("[12070] → {} TX: {}", peer, msg.trim());
        if let Err(e) = write_half.write_all(msg.as_bytes()).await {
            eprintln!("[12070] write error to {}: {}", peer, e);
            return;
        }
    }

    // Channel feeds both the periodic task and the db_watcher (QSO outbound).
    let (tx, mut rx) = mpsc::channel::<String>(64);
    peer_map.lock().await.insert(peer, tx.clone());

    let cfg2 = cfg.clone();
    let tx2  = tx.clone();
    drop(tx);
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(10));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let msgs = [
                msg_echoreq(&cfg2),
                msg_contestname(&cfg2),
                msg_status(&cfg2),
                msg_qsonrs(&cfg2),
            ];
            for msg in msgs {
                if tx2.send(msg).await.is_err() { return; }
            }
        }
    });

    let mut reader = FrameReader::new();
    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                println!("[12070] → {} TX: {}", peer, msg.trim());
                if let Err(e) = write_half.write_all(msg.as_bytes()).await {
                    eprintln!("[12070] write error to {}: {}", peer, e);
                    break;
                }
            }

            result = reader.next_frame(&mut read_half) => {
                match result {
                    Ok(Some(raw)) => {
                        println!("[12070] ← {} RX: {}", peer, raw.trim());
                        if let Some(msg) = parse_frame(&raw) {
                            if msg.command == "QSO" {
                                // Insert the contact from TCP QSO into fd_logger.db
                                if let Some(contact) = parse_tcp_qso(&msg.fields) {
                                    let db = cfg.db_path.clone();
                                    tokio::task::spawn_blocking(move || {
                                        match db_insert_contact(&db, contact) {
                                            Ok(true)  => println!("[12070]   QSO → inserted"),
                                            Ok(false) => println!("[12070]   QSO → duplicate"),
                                            Err(e)    => eprintln!("[12070]   QSO DB error: {}", e),
                                        }
                                    });
                                }
                            } else if let Some(reply) = handle_incoming(&msg, &cfg, peer) {
                                println!("[12070] → {} TX: {}", peer, reply.trim());
                                if let Err(e) = write_half.write_all(reply.as_bytes()).await {
                                    eprintln!("[12070] write error to {}: {}", peer, e);
                                    break;
                                }
                            }
                        } else {
                            println!("[12070] {} unparseable frame: {:?}", peer, raw.trim());
                        }
                    }
                    Ok(None) => { println!("[12070] {} disconnected", peer); break; }
                    Err(e)   => { eprintln!("[12070] read error from {}: {}", peer, e); break; }
                }
            }
        }
    }

    peer_map.lock().await.remove(&peer);

    // If this was an outbound connection, unregister so we can reconnect later.
    if let Some(peers) = outbound_peers {
        peers.lock().await.remove(&peer.ip());
    }
}

/// Process one incoming message and return a reply to send, if any.
/// Synchronous — no async needed, and avoids feeding replies back through
/// the mpsc channel (which would risk deadlock since the consumer is the
/// same task calling this function).
fn handle_incoming(msg: &InMsg, cfg: &Config, peer: SocketAddr) -> Option<String> {
    println!("[12070] ← {} cmd={} fields={:?}", msg.sender, msg.command, msg.fields);
    match msg.command.as_str() {
        "ECHOREQ" => {
            let date = msg.fields.first().map(String::as_str).unwrap_or("");
            let time = msg.fields.get(1).map(String::as_str).unwrap_or("");
            Some(msg_echo(cfg, date, time))
        }
        // Informational — no response needed
        "ECHO" | "LASTQAT" | "CONTESTNAME" | "STATUS" | "QSONRS"
        | "ReserveNr" | "XMIT" => None,
        other => {
            println!("[12070] ← {} unhandled command: {}", peer, other);
            None
        }
    }
}

// ── TCP listener ──────────────────────────────────────────────────────────────

async fn run_tcp_listener(cfg: Arc<Config>, peer_map: PeerMap) {
    let addr = format!("0.0.0.0:{}", TCP_PORT);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l)  => { println!("[12070] TCP listening on {}", addr); l }
        Err(e) => { eprintln!("[12070] Cannot bind TCP {}: {}", addr, e); return; }
    };

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let cfg      = cfg.clone();
                let peer_map = peer_map.clone();
                tokio::spawn(handle_connection(stream, peer, cfg, None, peer_map));
            }
            Err(e) => eprintln!("[12070] accept error: {}", e),
        }
    }
}

// ── UDP broadcaster ───────────────────────────────────────────────────────────

async fn run_udp_broadcaster(cfg: Arc<Config>) {
    let sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s)  => s,
        Err(e) => { eprintln!("[12070] Cannot create UDP socket: {}", e); return; }
    };
    if let Err(e) = sock.set_broadcast(true) {
        eprintln!("[12070] set_broadcast failed: {}", e);
    }

    let dest = format!("{}:{}", cfg.broadcast, UDP_PORT);
    let mut ticker = interval(Duration::from_secs(20));

    println!("[12070] UDP broadcaster → {} every 20s", dest);
    loop {
        ticker.tick().await;
        let payload = udp_announce(&cfg);
        match sock.send_to(payload.as_bytes(), &dest).await {
            Ok(n)  => println!("[12070] → UDP announce {} bytes", n),
            Err(e) => eprintln!("[12070] UDP send error: {}", e),
        }
    }
}

// ── UDP listener (discover N1MM peers) ───────────────────────────────────────

async fn run_udp_listener(cfg: Arc<Config>, peers: ConnectedPeers, peer_map: PeerMap) {
    let addr = format!("0.0.0.0:{}", UDP_PORT);
    let sock = match UdpSocket::bind(&addr).await {
        Ok(s)  => { println!("[12070] UDP listening on {}", addr); s }
        Err(e) => { eprintln!("[12070] Cannot bind UDP {}: {}", addr, e); return; }
    };
    if let Err(e) = sock.set_broadcast(true) {
        eprintln!("[12070] set_broadcast failed: {}", e);
    }

    let mut buf = vec![0u8; 1024];
    loop {
        match sock.recv_from(&mut buf).await {
            Ok((n, _from)) => {
                let text = String::from_utf8_lossy(&buf[..n]);
                if text.starts_with(cfg.station.as_str()) {
                    continue; // ignore our own announces
                }
                // Parse: station%ip%port%version%...
                let parts: Vec<&str> = text.trim().split('%').collect();
                if parts.len() < 3 { continue; }
                let peer_ip: IpAddr = match parts[1].parse() {
                    Ok(ip) => ip,
                    Err(_) => continue,
                };
                let peer_port: u16 = parts[2].parse().unwrap_or(TCP_PORT);

                // Connect outbound only if we don't already have an active
                // connection to this peer.  Without an outbound connection,
                // N1MM shows "Received FAIL" because it tracks inbound TCP
                // connections from peers as the "received" health indicator.
                {
                    let mut locked = peers.lock().await;
                    if locked.contains(&peer_ip) {
                        continue; // already connected
                    }
                    locked.insert(peer_ip);
                }

                let peer_addr = format!("{}:{}", peer_ip, peer_port);
                println!("[12070] → outbound TCP to {}", peer_addr);
                let cfg2      = cfg.clone();
                let peers2    = peers.clone();
                let peer_map2 = peer_map.clone();
                tokio::spawn(async move {
                    match TcpStream::connect(&peer_addr).await {
                        Ok(stream) => {
                            let sa = stream.peer_addr()
                                .unwrap_or_else(|_| peer_addr.parse().unwrap());
                            handle_connection(stream, sa, cfg2, Some(peers2), peer_map2).await;
                        }
                        Err(e) => {
                            eprintln!("[12070] outbound connect to {} failed: {}", peer_addr, e);
                            peers2.lock().await.remove(&peer_ip);
                        }
                    }
                });
            }
            Err(e) => eprintln!("[12070] UDP recv error: {}", e),
        }
    }
}

// ── Port 12060 XML contact listener ──────────────────────────────────────────
//
// N1MM+ broadcasts each logged QSO as an XML UDP datagram on port 12060.
// We parse it and insert it into fd_logger.db so the web UI stays in sync.

#[derive(Debug)]
struct XmlContact {
    call:     String,
    band:     String,
    mode:     String,
    class:    String,
    section:  String,
    operator: String,
    date:     String,
    time:     String,
    n1mm_id:  String,
}

/// Map N1MM band field to fd_logger's band string.
/// Handles integer meters ("20"), kHz freq ("14200.00"), MHz freq ("14.0"), or
/// 10 Hz units ("1420000") as observed across different N1MM versions/contexts.
fn band_from_n1mm(s: &str) -> Option<String> {
    match s.trim() {
        "160" => return Some("160M".into()), "80" => return Some("80M".into()),
        "60"  => return Some("60M".into()),  "40" => return Some("40M".into()),
        "30"  => return Some("30M".into()),  "20" => return Some("20M".into()),
        "17"  => return Some("17M".into()),  "15" => return Some("15M".into()),
        "12"  => return Some("12M".into()),  "10" => return Some("10M".into()),
        "6"   => return Some("6M".into()),   "2"  => return Some("2M".into()),
        "0.7" | "70cm" | "70CM" => return Some("70CM".into()),
        _ => {}
    }
    let f: f64 = s.trim().parse().ok()?;
    // ≥ 100 000 → 10 Hz units (N1MM XML rxfreq style); ≥ 1 000 → kHz; else MHz
    let khz = if f >= 100_000.0 { f / 100.0 }
              else if f >= 1_000.0 { f }
              else { f * 1_000.0 };
    band_from_khz(khz as u32)
}

/// Map N1MM mode string to fd_logger's PH / CW / DIG.
fn mode_from_n1mm(s: &str) -> String {
    match s.trim().to_uppercase().as_str() {
        "CW"                       => "CW".into(),
        "USB" | "LSB" | "FM" | "AM" => "PH".into(),
        _                           => "DIG".into(),
    }
}

/// Parse an N1MM+ <contactinfo> XML datagram into an XmlContact.
/// Returns None for non-contact messages (contactreplace, contactdelete, etc.)
/// or packets missing required fields.
fn parse_xml_contact(xml: &str) -> Option<XmlContact> {
    let doc = roxmltree::Document::parse(xml).ok()?;
    let root = doc.root_element();

    if root.tag_name().name() != "contactinfo" {
        return None;
    }

    let get = |tag: &str| -> String {
        doc.descendants()
            .find(|n| n.tag_name().name() == tag)
            .and_then(|n| n.text())
            .unwrap_or("")
            .trim()
            .to_string()
    };

    let call    = get("call");
    let n1mm_id = get("ID");
    if call.is_empty() || n1mm_id.is_empty() {
        return None;
    }

    let band = band_from_n1mm(&get("band"))?;
    let mode = mode_from_n1mm(&get("mode"));

    // timestamp field: "YYYY-MM-DD HH:MM:SS"
    let ts = get("timestamp");
    let (date, time) = match ts.split_once(' ') {
        Some((d, t)) => (d.to_string(), t.get(..5).unwrap_or(t).to_string()),
        None         => (ts.clone(), "00:00".to_string()),
    };

    Some(XmlContact {
        call:     call.to_uppercase(),
        band,
        mode,
        class:    get("exchange1").to_uppercase(),
        section:  get("section").to_uppercase(),
        operator: get("operator").to_uppercase(),
        date,
        time,
        n1mm_id,
    })
}

/// Insert a contact into fd_logger.db if the n1mm_id is not already present.
/// Returns Ok(true) if inserted, Ok(false) if duplicate.
fn db_insert_contact(db_path: &str, c: XmlContact) -> rusqlite::Result<bool> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;

    let exists: bool = conn.query_row(
        "SELECT COUNT(*) FROM contacts WHERE n1mm_id = ?1",
        rusqlite::params![c.n1mm_id],
        |row| row.get::<_, i64>(0),
    ).unwrap_or(0) > 0;

    if exists {
        return Ok(false);
    }

    let now = Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO contacts
             (date, time, call, band, mode, class, section, operator, created_at, n1mm_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        rusqlite::params![
            c.date, c.time, c.call, c.band, c.mode,
            c.class, c.section, c.operator, now, c.n1mm_id
        ],
    )?;
    Ok(true)
}

async fn run_xml_listener(cfg: Arc<Config>) {
    let db_path = cfg.db_path.clone();
    let addr = "0.0.0.0:12060";
    let sock = match UdpSocket::bind(addr).await {
        Ok(s)  => { println!("[12060] UDP listening on {} (db: {})", addr, db_path); s }
        Err(e) => { eprintln!("[12060] Cannot bind UDP {}: {}", addr, e); return; }
    };

    let mut buf = vec![0u8; 65535];
    loop {
        match sock.recv_from(&mut buf).await {
            Ok((n, from)) => {
                let xml = String::from_utf8_lossy(&buf[..n]).to_string();
                println!("[12060] ← {} bytes from {}", n, from);
                match parse_xml_contact(&xml) {
                    Some(contact) => {
                        println!("[12060]   {} {} {} {} {}",
                            contact.call, contact.band, contact.mode,
                            contact.class, contact.section);
                        let db = db_path.clone();
                        tokio::task::spawn_blocking(move || {
                            match db_insert_contact(&db, contact) {
                                Ok(true)  => println!("[12060]   → inserted"),
                                Ok(false) => println!("[12060]   → duplicate, skipped"),
                                Err(e)    => eprintln!("[12060]   DB error: {}", e),
                            }
                        });
                    }
                    None => {
                        // Log the root tag name so we can see what we ignored
                        let tag = roxmltree::Document::parse(&xml).ok()
                            .map(|d| d.root_element().tag_name().name().to_string())
                            .unwrap_or_else(|| "<parse error>".into());
                        println!("[12060]   ignored <{}>", tag);
                    }
                }
            }
            Err(e) => eprintln!("[12060] recv error: {}", e),
        }
    }
}

// ── Port 12060 outbound — fd_logger contacts → N1MM peers ────────────────────

struct DbContact {
    id:       i64,
    call:     String,
    band:     String,
    mode:     String,
    class:    String,
    section:  String,
    operator: String,
    date:     String,
    time:     String,
}

/// Typical center frequency in kHz per Field Day band (for rxfreq in TCP QSO).
fn band_center_khz(b: &str) -> f64 {
    match b {
        "160M" => 1825.0, "80M"  => 3750.0, "60M"  => 5357.0,
        "40M"  => 7200.0, "30M"  => 10125.0,"20M"  => 14200.0,
        "17M"  => 18120.0,"15M"  => 21200.0,"12M"  => 24920.0,
        "10M"  => 28400.0,"6M"   => 50200.0,"2M"   => 144200.0,
        "70CM" => 432100.0, _ => 14200.0,
    }
}

/// Map frequency in kHz to fd_logger band string.
fn band_from_khz(khz: u32) -> Option<String> {
    match khz {
        1800..=2000     => Some("160M".into()),
        3500..=4000     => Some("80M".into()),
        5330..=5407     => Some("60M".into()),
        7000..=7300     => Some("40M".into()),
        10100..=10150   => Some("30M".into()),
        14000..=14350   => Some("20M".into()),
        18068..=18168   => Some("17M".into()),
        21000..=21450   => Some("15M".into()),
        24890..=24990   => Some("12M".into()),
        28000..=29700   => Some("10M".into()),
        50000..=54000   => Some("6M".into()),
        144000..=148000 => Some("2M".into()),
        420000..=450000 => Some("70CM".into()),
        _               => None,
    }
}

/// Build a <contactinfo> XML datagram for port 12060 UDP broadcast.
/// rxfreq/txfreq are in 10 Hz units (kHz × 100) as observed in N1MM captures.
fn build_contactinfo_xml(cfg: &Config, c: &DbContact, n1mm_id: &str) -> String {
    let freq_10hz = (band_center_khz(&c.band) * 100.0) as u64;
    let band_mhz  = band_mhz_str(&c.band);
    let mode_n    = mode_to_n1mm(&c.mode);
    let ts        = format!("{} {}:00", c.date, c.time);
    let prefix: String = c.call.chars().take_while(|ch| ch.is_ascii_alphabetic()).collect();

    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<contactinfo>\n\
\t<app>N1MM</app>\n\
\t<contestname>{contest}</contestname>\n\
\t<contestnr>1</contestnr>\n\
\t<timestamp>{ts}</timestamp>\n\
\t<mycall>{mycall}</mycall>\n\
\t<band>{band}</band>\n\
\t<rxfreq>{freq}</rxfreq>\n\
\t<txfreq>{freq}</txfreq>\n\
\t<operator>{op}</operator>\n\
\t<mode>{mode}</mode>\n\
\t<call>{call}</call>\n\
\t<countryprefix>K</countryprefix>\n\
\t<wpxprefix>{prefix}</wpxprefix>\n\
\t<stationprefix>{mycall}</stationprefix>\n\
\t<continent>NA</continent>\n\
\t<snt>59</snt><sntnr>1</sntnr>\n\
\t<rcv>59</rcv><rcvnr>{class}</rcvnr>\n\
\t<gridsquare></gridsquare>\n\
\t<exchange1>{class}</exchange1>\n\
\t<section>{section}</section>\n\
\t<comment></comment>\n\
\t<qth></qth><name></name><power></power>\n\
\t<misctext></misctext>\n\
\t<zone>5</zone><prec></prec><ck>0</ck>\n\
\t<ismultiplier1>0</ismultiplier1>\n\
\t<ismultiplier2>0</ismultiplier2>\n\
\t<ismultiplier3>0</ismultiplier3>\n\
\t<points>1</points>\n\
\t<radionr>1</radionr>\n\
\t<run1run2>1</run1run2>\n\
\t<RoverLocation></RoverLocation>\n\
\t<RadioInterfaced>0</RadioInterfaced>\n\
\t<NetworkedCompNr>0</NetworkedCompNr>\n\
\t<IsOriginal>True</IsOriginal>\n\
\t<NetBiosName>{station}</NetBiosName>\n\
\t<IsRunQSO>0</IsRunQSO>\n\
\t<StationName>{station}</StationName>\n\
\t<ID>{id}</ID>\n\
\t<IsClaimedQso>1</IsClaimedQso>\n\
\t<SentExchange>1A MDC</SentExchange>\n\
</contactinfo>",
        contest = cfg.contest,
        ts      = ts,
        mycall  = cfg.callsign,
        band    = band_mhz,
        freq    = freq_10hz,
        op      = c.operator,
        mode    = mode_n,
        call    = c.call,
        prefix  = prefix,
        class   = c.class,
        section = c.section,
        id      = n1mm_id,
        station = cfg.station,
    )
}

fn db_fetch_unbroadcast(db_path: &str, since_id: i64) -> rusqlite::Result<Vec<DbContact>> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
    let mut stmt = conn.prepare(
        "SELECT id, call, band, mode, class, section, operator, date, time
         FROM contacts WHERE id > ?1 AND n1mm_id IS NULL ORDER BY id ASC",
    )?;
    let rows = stmt.query_map(rusqlite::params![since_id], |row| {
        Ok(DbContact {
            id: row.get(0)?, call: row.get(1)?, band: row.get(2)?,
            mode: row.get(3)?, class: row.get(4)?, section: row.get(5)?,
            operator: row.get(6)?, date: row.get(7)?, time: row.get(8)?,
        })
    })?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn db_stamp_n1mm_id(db_path: &str, id: i64, n1mm_id: &str) -> rusqlite::Result<()> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
    conn.execute(
        "UPDATE contacts SET n1mm_id = ?1 WHERE id = ?2 AND n1mm_id IS NULL",
        rusqlite::params![n1mm_id, id],
    )?;
    Ok(())
}

/// Derive subnet broadcast address from a local IP (assumes /24).
fn subnet_broadcast(local_ip: &str) -> String {
    let parts: Vec<&str> = local_ip.split('.').collect();
    if parts.len() == 4 {
        format!("{}.{}.{}.255", parts[0], parts[1], parts[2])
    } else {
        "255.255.255.255".to_string()
    }
}

async fn run_db_watcher(cfg: Arc<Config>, peer_map: PeerMap) {
    let db_path   = cfg.db_path.clone();
    // Use subnet broadcast (192.168.x.255) matching real N1MM behavior.
    let udp_dest  = format!("{}:12060", subnet_broadcast(&cfg.local_ip));

    let udp_sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s)  => s,
        Err(e) => { eprintln!("[12060] watcher: cannot create UDP socket: {}", e); return; }
    };
    udp_sock.set_broadcast(true).ok();

    // Baseline: only broadcast contacts logged AFTER bridge starts.
    let db = db_path.clone();
    let mut last_id: i64 = tokio::task::spawn_blocking(move || {
        rusqlite::Connection::open(&db).ok()
            .and_then(|c| c.query_row(
                "SELECT COALESCE(MAX(id), 0) FROM contacts", [],
                |r| r.get::<_, i64>(0)).ok())
            .unwrap_or(0)
    }).await.unwrap_or(0);
    println!("[12060] watcher started, baseline id={}", last_id);

    let mut ticker = interval(Duration::from_secs(1));
    loop {
        ticker.tick().await;

        let db    = db_path.clone();
        let since = last_id;
        let contacts = match tokio::task::spawn_blocking(
            move || db_fetch_unbroadcast(&db, since)).await
        {
            Ok(Ok(v))  => v,
            Ok(Err(e)) => { eprintln!("[12060] watcher db error: {}", e); continue; }
            Err(e)     => { eprintln!("[12060] watcher task error: {}", e); continue; }
        };

        for c in contacts {
            last_id = last_id.max(c.id);
            let n1mm_id = format!("{:016x}{:016x}",
                Local::now().timestamp() as u64, c.id as u64);

            // 1. Send TCP QSO to every connected N1MM peer.
            let qso_msg = msg_qso_contact(&cfg, &c, &n1mm_id);
            let snapshot: Vec<_> = {
                let map = peer_map.lock().await;
                map.iter().map(|(a, tx)| (*a, tx.clone())).collect()
            };
            for (addr, tx) in &snapshot {
                println!("[12060] → TCP QSO to {} {} {} {} {}",
                    addr, c.call, c.band, c.mode, c.class);
                if tx.send(qso_msg.clone()).await.is_err() {
                    eprintln!("[12060] watcher: peer {} channel closed", addr);
                }
            }

            // 2. UDP 12060 XML broadcast so other stations on the LAN also receive it.
            let xml = build_contactinfo_xml(&cfg, &c, &n1mm_id);
            println!("[12060] → UDP XML to {}", udp_dest);
            if let Err(e) = udp_sock.send_to(xml.as_bytes(), &udp_dest).await {
                eprintln!("[12060] watcher UDP send error: {}", e);
            }

            // Stamp n1mm_id to prevent re-broadcast and suppress echo-back duplicate.
            let db2 = db_path.clone(); let id2 = c.id; let nid = n1mm_id.clone();
            tokio::task::spawn_blocking(move || {
                if let Err(e) = db_stamp_n1mm_id(&db2, id2, &nid) {
                    eprintln!("[12060] watcher stamp error: {}", e);
                }
            });
        }
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let cfg  = Arc::new(Config::from_args(&args));

    println!("N1MM Bridge starting");
    println!("  Station : {}", cfg.station);
    println!("  Callsign: {}", cfg.callsign);
    println!("  Contest : {}", cfg.contest);
    println!("  Local IP: {}", cfg.local_ip);
    println!("  DB      : {}", cfg.db_path);

    let outbound_peers: ConnectedPeers = Arc::new(TokioMutex::new(HashSet::new()));
    let peer_map:       PeerMap        = Arc::new(TokioMutex::new(HashMap::new()));
    println!("  Subnet broadcast: {}", subnet_broadcast(&cfg.local_ip));

    let t1 = tokio::spawn(run_tcp_listener(cfg.clone(), peer_map.clone()));
    let t2 = tokio::spawn(run_udp_broadcaster(cfg.clone()));
    let t3 = tokio::spawn(run_udp_listener(cfg.clone(), outbound_peers.clone(), peer_map.clone()));
    let t4 = tokio::spawn(run_xml_listener(cfg.clone()));
    let t5 = tokio::spawn(run_db_watcher(cfg.clone(), peer_map.clone()));

    tokio::select! {
        _ = t1 => eprintln!("TCP listener exited"),
        _ = t2 => eprintln!("UDP broadcaster exited"),
        _ = t3 => eprintln!("UDP listener exited"),
        _ = t4 => eprintln!("XML listener exited"),
        _ = t5 => eprintln!("DB watcher exited"),
    }
}
