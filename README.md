# N1MM Bridge

A bridge daemon that connects [N1MM+](https://n1mm.hamdocs.com/) logging software to
[FD Logger](https://github.com/kb3gtn/FD_LOGGER) by implementing the N1MM+ LAN peer
protocol. Run it alongside FD Logger during Field Day to provide **full bidirectional
contact sync** between FD Logger and all N1MM+ stations on the local network.

## How it works

N1MM+ uses two protocols to network logging stations:

| Port | Protocol | Purpose |
|------|----------|---------|
| 12070 UDP | Broadcast announce | Peer discovery — each station announces itself every ~20 seconds |
| 12070 TCP | Framed messages | Keepalive, status, QSO count exchange, and contact sync between peers |
| 12060 UDP | XML broadcast | Contact sync — logged QSOs broadcast to all peers |

`n1mm_bridge` implements all three and provides full bidirectional sync:

- **N1MM+ → FD Logger**: Contacts logged in any N1MM+ station appear immediately in the
  FD Logger web UI.  Received via TCP QSO messages (port 12070) and UDP XML broadcasts
  (port 12060).
- **FD Logger → N1MM+**: Contacts logged in the FD Logger web UI are sent to all
  connected N1MM+ stations within one second via TCP QSO and UDP XML broadcast.
- **Peer health**: The bridge initiates outbound TCP connections to each discovered peer
  (matching real N1MM+ behavior), so N1MM+ shows send/receive OK for the FD Logger station.

## Requirements

- Rust 1.75 or later
- Samba / NetBIOS running on the same machine (so N1MM+ can discover the host by name)
- UDP and TCP port 12070 open inbound in the firewall
- UDP port 12060 open inbound in the firewall

### Firewall (firewalld)

```bash
sudo firewall-cmd --add-port=12070/tcp --permanent
sudo firewall-cmd --add-port=12070/udp --permanent
sudo firewall-cmd --add-port=12060/udp --permanent
sudo firewall-cmd --reload
```

## Building

```bash
git clone https://github.com/kb3gtn/n1mm_bridge.git
cd n1mm_bridge
cargo build --release
```

The binary is written to `target/release/n1mm_bridge`.

## Running

Place the binary in the same directory as `fd_logger.db` (the FD Logger database file),
then run:

```bash
./target/release/n1mm_bridge \
  --callsign KB3GTN \
  --station WA3NAN-REMOTE \
  --local-ip 192.168.1.17
```

`--station` must match the machine's NetBIOS name as advertised by Samba. Check it with:

```bash
nmblookup -A <your-ip>
```

### Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--callsign` | `-c` | *(required)* | Station callsign |
| `--station` | `-s` | `FDLOGGER` | NetBIOS name advertised to N1MM+ peers |
| `--contest` | | `FD` | Contest name sent in status messages |
| `--local-ip` | `-i` | *(required)* | Local IP address to include in UDP announces |
| `--broadcast` | | `255.255.255.255` | UDP broadcast address for peer announces |
| `--db` | `-d` | `fd_logger.db` | Path to the FD Logger SQLite database |

The subnet broadcast for port 12060 XML is derived automatically from `--local-ip`
(e.g. `192.168.1.17` → `192.168.1.255`) to match real N1MM+ behavior.

## Protocol notes

All protocol details were reverse-engineered from Wireshark captures of live N1MM+
traffic between real stations.

### Port 12070 — peer protocol

- **UDP announces** are sent every 20 seconds in the format:
  `<station>%<ip>%<port>%<version>%<station>%%`
- **TCP messages** use the frame format:
  `DATA__00%<sender>%<command>%<fields>%~__DATA`
- **Dual TCP connections** are required: N1MM+ tracks inbound connections from peers as
  the "Received" health indicator.  The bridge initiates one outbound connection per peer
  on discovery, exactly as real N1MM+ stations do.
- **Handshake order**: only `QSONRS` + `LASTQAT` are sent on connect; `ECHOREQ`,
  `CONTESTNAME`, and `STATUS` are sent by the periodic timer (~10 s later).
- **QSONRS** must contain exactly 33 data fields or N1MM+ raises an `InvalidCastException`.
- **GUID format**: N1MM contact IDs must be 32 valid hex characters.
- **Timestamps**: local time (not UTC) is used in `ECHOREQ` and `LASTQAT`.
- Multiple messages per TCP segment are handled correctly via a stateful frame reader.

### Port 12060 — contact XML broadcast

**Receiving (N1MM+ → FD Logger):** `n1mm_bridge` listens for `<contactinfo>` UDP
packets and extracts:

| XML field | Maps to |
|-----------|---------|
| `<call>` | contacted station callsign |
| `<band>` | MHz float, kHz, or meters string — all formats handled |
| `<mode>` | USB/LSB/FM → `PH`, CW → `CW`, all others → `DIG` |
| `<exchange1>` | received Field Day class (e.g. `1A`) |
| `<section>` | received ARRL section (e.g. `EPA`) |
| `<operator>` | operator callsign |
| `<timestamp>` | QSO date and time |
| `<ID>` | N1MM GUID — used for duplicate detection |

**Sending (FD Logger → N1MM+):** When a contact is logged in the FD Logger web UI,
the bridge broadcasts a `<contactinfo>` XML datagram on the subnet broadcast address.
Key format details from captures:

- `<rxfreq>` / `<txfreq>` are in **10 Hz units** (kHz × 100): 20M = `1420000`
- `<band>` is an **MHz float string**: `3.5`, `14.0`, `21.0`, etc.

Each contact is stamped with a generated GUID after sending so retransmissions and
echo-backs do not create duplicate entries.

### TCP QSO — contact sync over port 12070

In addition to UDP XML, N1MM+ sends a `QSO` command over the TCP peer connection
whenever a contact is logged.  `n1mm_bridge` both sends and receives this command:

- **Receiving**: TCP QSO messages from N1MM+ peers are parsed and inserted into
  `fd_logger.db` (same duplicate-detection logic as UDP 12060).
- **Sending**: When fd_logger logs a contact, the bridge sends a TCP QSO to every
  connected peer so N1MM+ receives and relays the contact immediately.

## Relationship to FD Logger

`n1mm_bridge` is a companion to [FD Logger](https://github.com/kb3gtn/FD_LOGGER).
They are separate binaries that share the `fd_logger.db` SQLite database (opened in
WAL mode so both processes can access it concurrently without blocking each other).
FD Logger handles the web UI and direct contact entry; n1mm_bridge handles N1MM+
network integration.

## Planned features

- Live QSONRS counts read from `fd_logger.db` instead of all-zeros

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE) for details.
