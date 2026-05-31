# N1MM Bridge

A bridge daemon that connects [N1MM+](https://n1mm.hamdocs.com/) logging software to
[FD Logger](https://github.com/kb3gtn/FD_LOGGER) by implementing the N1MM+ LAN peer
protocol. Run it alongside FD Logger during Field Day to allow N1MM+ stations on the
local network to see FD Logger as a peer.

## How it works

N1MM+ uses two protocols to network logging stations:

| Port | Protocol | Purpose |
|------|----------|---------|
| 12070 UDP | Broadcast announce | Peer discovery — each station announces itself every ~20 seconds |
| 12070 TCP | Framed messages | Keepalive, status, and QSO count exchange between peers |
| 12060 UDP | XML broadcast | Contact sync — logged QSOs broadcast to all peers |

`n1mm_bridge` implements the port 12070 peer protocol so that FD Logger appears in
N1MM+'s network station list with send/receive OK status. Port 12060 contact sync is
planned for a future release.

## Requirements

- Rust 1.75 or later
- Samba / NetBIOS running on the same machine (so N1MM+ can discover the host by name)
- UDP and TCP port 12070 open inbound in the firewall

### Firewall (firewalld)

```bash
sudo firewall-cmd --add-port=12070/tcp --permanent
sudo firewall-cmd --add-port=12070/udp --permanent
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

## Protocol notes

The N1MM+ port 12070 protocol was reverse-engineered from Wireshark captures of live
N1MM+ traffic. Key implementation details:

- **UDP announces** are sent every 20 seconds in the format:
  `<station>%<ip>%<port>%<version>%<station>%%`
- **TCP messages** use the frame format:
  `DATA__00%<sender>%<command>%<fields>%~__DATA`
- **QSONRS** must contain exactly 33 data fields or N1MM+ will raise a parse exception
- Multiple messages per TCP segment are handled correctly via a stateful frame reader

## Relationship to FD Logger

`n1mm_bridge` is a companion to [FD Logger](https://github.com/kb3gtn/FD_LOGGER).
They are separate binaries that share the `fd_logger.db` SQLite database. FD Logger
handles the web UI and contact entry; n1mm_bridge handles external radio software
integration.

## Planned features

- Port 12060 UDP listener — receive N1MM+ contact broadcasts and write them to `fd_logger.db`
- Live QSONRS counts read from `fd_logger.db` instead of all-zeros

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE) for details.
