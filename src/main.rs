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

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::mpsc;
use tokio::time::interval;

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
    let now = Utc::now();
    (now.format("%Y-%m-%d").to_string(), now.format("%H:%M:%S").to_string())
}

/// UDP announce packet: <station>%<ip>%<port>%<version>%<station>%%
fn udp_announce(cfg: &Config) -> String {
    format!("{}%{}%{}%{}%{}%%",
        cfg.station, cfg.local_ip, TCP_PORT, N1MM_VERSION, cfg.station)
}

fn msg_lastqat(cfg: &Config) -> String {
    // TODO: read actual last QSO time from fd_logger.db
    let now = Utc::now();
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

async fn handle_connection(stream: TcpStream, peer: SocketAddr, cfg: Arc<Config>) {
    println!("[12070] connected: {}", peer);

    let (mut read_half, mut write_half) = stream.into_split();

    // Send initial handshake burst
    let handshake = [
        msg_lastqat(&cfg),
        msg_echoreq(&cfg),
        msg_contestname(&cfg),
        msg_status(&cfg),
    ];
    for msg in &handshake {
        println!("[12070] → {} TX: {}", peer, msg.trim());
        if let Err(e) = write_half.write_all(msg.as_bytes()).await {
            eprintln!("[12070] write error to {}: {}", peer, e);
            return;
        }
    }

    // Periodic messages flow through this channel to the write loop
    let (tx, mut rx) = mpsc::channel::<String>(32);

    // Spawn periodic sender (every 10 seconds)
    let cfg2 = cfg.clone();
    let tx2  = tx.clone();
    drop(tx); // only tx2 is used; drop original so channel closes when periodic task ends
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(10));
        ticker.tick().await; // skip the first immediate tick
        loop {
            ticker.tick().await;
            let msgs = [
                msg_echoreq(&cfg2),
                msg_contestname(&cfg2),
                msg_status(&cfg2),
                msg_qsonrs(&cfg2),
            ];
            for msg in msgs {
                if tx2.send(msg).await.is_err() {
                    return; // receiver gone — connection closed
                }
            }
        }
    });

    // Main read/write loop
    let mut reader = FrameReader::new();
    loop {
        tokio::select! {
            // Drain and send outgoing messages
            Some(msg) = rx.recv() => {
                println!("[12070] → {} TX: {}", peer, msg.trim());
                if let Err(e) = write_half.write_all(msg.as_bytes()).await {
                    eprintln!("[12070] write error to {}: {}", peer, e);
                    break;
                }
            }

            // Read next incoming message (one at a time, even if N1MM batched several)
            result = reader.next_frame(&mut read_half) => {
                match result {
                    Ok(Some(raw)) => {
                        println!("[12070] ← {} RX: {}", peer, raw.trim());
                        if let Some(msg) = parse_frame(&raw) {
                            if let Some(reply) = handle_incoming(&msg, &cfg, peer) {
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
                    Ok(None) => {
                        println!("[12070] {} disconnected", peer);
                        break;
                    }
                    Err(e) => {
                        eprintln!("[12070] read error from {}: {}", peer, e);
                        break;
                    }
                }
            }
        }
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

async fn run_tcp_listener(cfg: Arc<Config>) {
    let addr = format!("0.0.0.0:{}", TCP_PORT);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l)  => { println!("[12070] TCP listening on {}", addr); l }
        Err(e) => { eprintln!("[12070] Cannot bind TCP {}: {}", addr, e); return; }
    };

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let cfg = cfg.clone();
                tokio::spawn(handle_connection(stream, peer, cfg));
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

async fn run_udp_listener(cfg: Arc<Config>) {
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
            Ok((n, from)) => {
                let text = String::from_utf8_lossy(&buf[..n]);
                // Ignore our own announces
                if text.starts_with(cfg.station.as_str()) {
                    continue;
                }
                println!("[12070] ← UDP announce from {}: {}", from, text.trim());
                // N1MM connects to us (inbound) after seeing our UDP announces.
                // We do not initiate outbound connections — doing so on every
                // 20-second announce cycle would create duplicate connections that
                // N1MM drops with "other end closed connection" errors.
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

/// Map N1MM band-in-meters string to fd_logger's uppercase "NNM" format.
fn band_from_n1mm(s: &str) -> Option<String> {
    match s.trim() {
        "160" => Some("160M".into()),
        "80"  => Some("80M".into()),
        "60"  => Some("60M".into()),
        "40"  => Some("40M".into()),
        "30"  => Some("30M".into()),
        "20"  => Some("20M".into()),
        "17"  => Some("17M".into()),
        "15"  => Some("15M".into()),
        "12"  => Some("12M".into()),
        "10"  => Some("10M".into()),
        "6"   => Some("6M".into()),
        "2"   => Some("2M".into()),
        "0.7" | "70cm" | "70CM" => Some("70CM".into()),
        _     => None,
    }
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

    let t1 = tokio::spawn(run_tcp_listener(cfg.clone()));
    let t2 = tokio::spawn(run_udp_broadcaster(cfg.clone()));
    let t3 = tokio::spawn(run_udp_listener(cfg.clone()));
    let t4 = tokio::spawn(run_xml_listener(cfg.clone()));

    tokio::select! {
        _ = t1 => eprintln!("TCP listener exited"),
        _ = t2 => eprintln!("UDP broadcaster exited"),
        _ = t3 => eprintln!("UDP listener exited"),
        _ = t4 => eprintln!("XML listener exited"),
    }
}
