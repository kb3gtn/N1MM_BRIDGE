# N1MM Bridge

A bridge daemon that connects [N1MM+](https://n1mm.hamdocs.com/) logging software to
[FD Logger](https://github.com/kb3gtn/FD_LOGGER) by implementing the N1MM+ LAN peer
protocol. Run it alongside FD Logger during Field Day to allow N1MM+ stations on the
local network to see FD Logger as a peer and have their logged contacts written into
the FD Logger database automatically.

## How it works

N1MM+ uses two protocols to network logging stations:

| Port | Protocol | Purpose |
|------|----------|---------|
| 12070 UDP | Broadcast announce | Peer discovery — each station announces itself every ~20 seconds |
| 12070 TCP | Framed messages | Keepalive, status, and QSO count exchange between peers |
| 12060 UDP | XML broadcast | Contact sync — logged QSOs broadcast to all peers |

`n1mm_bridge` implements both protocols:

- **Port 12070** — peer discovery and keepalive so FD Logger appears in N1MM+'s network
  station list with send/receive OK status.
- **Port 12060** — listens for N1MM+ contact XML broadcasts and writes each new QSO into
  `fd_logger.db`, making contacts logged from any N1MM+ station immediately visible in the
  FD Logger web UI.

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

## Protocol notes

### Port 12070 — peer protocol

The N1MM+ port 12070 protocol was reverse-engineered from Wireshark captures of live
N1MM+ traffic. Key implementation details:

- **UDP announces** are sent every 20 seconds in the format:
  `<station>%<ip>%<port>%<version>%<station>%%`
- **TCP messages** use the frame format:
  `DATA__00%<sender>%<command>%<fields>%~__DATA`
- **QSONRS** must contain exactly 33 data fields or N1MM+ will raise a parse exception
- Multiple messages per TCP segment are handled correctly via a stateful frame reader

### Port 12060 — contact XML broadcast

N1MM+ broadcasts each logged QSO as a UDP datagram containing an XML document on port
12060. `n1mm_bridge` listens for `<contactinfo>` packets and extracts:

| XML field | Maps to |
|-----------|---------|
| `<call>` | contacted station callsign |
| `<band>` | band in meters (e.g. `20` → `20M`) |
| `<mode>` | USB/LSB/FM → `PH`, CW → `CW`, all others → `DIG` |
| `<exchange1>` | received Field Day class (e.g. `1A`) |
| `<section>` | received ARRL section (e.g. `EPA`) |
| `<operator>` | operator callsign |
| `<timestamp>` | QSO date and time |
| `<ID>` | N1MM GUID — used for duplicate detection |

Each contact is inserted into `fd_logger.db` only if its `<ID>` has not been seen
before, so restarting the bridge or receiving retransmitted packets is safe.
Other message types (`<contactreplace>`, `<contactdelete>`, score packets, etc.)
are ignored and logged by tag name.

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
