# NetBond

Combine Wi-Fi, mobile data and Ethernet/USB on an Android phone into one faster,
more reliable connection, using your own server.

It has two modes, chosen with the "Use my own server" switch:

- **Without a server (default).** Nothing to set up. Each new connection goes
  out over one network, taking turns. Apps that open several connections
  (browsers, download managers, torrents, most speed tests) get faster; a
  single connection is no faster than the network it landed on.
- **With your own server.** Every single connection is split across all your
  networks at once and put back together on your server, so even one download
  or one video stream uses everything. If a network drops, connections carry
  on over the others.

```
 apps ─► VPN interface ─► NetBond app ══ Wi-Fi ═══════╗
                                      ══ mobile data ══╬═► your server ─► internet
                                      ══ Ethernet/USB ═╝
```

## Set up (server mode)

Skip this for no-server mode: install the APK, leave the switch off, tap **Connect**.

**1. Server** (any Linux VPS, 1 CPU / 512 MB is enough). As root:

```
curl -fsSL https://github.com/Mustafaakreyi1995/netbond/releases/latest/download/install.sh | bash
```

It prints a connection code that starts with `netbond://`. Keep it private:
anyone who has it can use your server.

**2. Phone.** Download `NetBond.apk` from the
[latest release](https://github.com/Mustafaakreyi1995/netbond/releases/latest), install it,
paste the code, tap **Connect**, and accept Android's VPN prompt.

Mobile data must be switched on. The app keeps it active alongside Wi-Fi.

## What to expect (server mode)

- Speed is roughly the sum of your networks, capped by your server's own
  bandwidth. Pick a server close to you; every packet makes the detour.
- Websites see your server's IP address, not your phone's.
- Mobile data is used whenever the VPN is on, including for large downloads.
- UDP (calls, games, QUIC) is tunnelled inside TCP links, so it works but gains
  no speed and can lag on a congested link.

## What to expect (no-server mode)

- Websites see two different IP addresses from you. A few sites that tie a
  login to one address (some banks, some games) may log you out or misbehave.
- Mobile data is used whenever the VPN is on.

## Layout

| Path | What |
|---|---|
| `core/bond` | Multipath protocol: framing, scheduler, failover, client, server |
| `core/mobile` | Android-facing API and the userspace TCP/IP stack on the VPN interface |
| `core/cmd/netbond-server` | The server program |
| `android` | The app (Kotlin) |
| `server/install.sh` | VPS installer |

The link between phone and server is TLS 1.3. Both ends derive the server's key
and the client's login token from the shared secret in the connection code, so
no certificates need managing.

Everything is built by GitHub Actions (`.github/workflows/build.yml`) on each
push to `main` and published as a release.

## Server commands

```
systemctl status netbond        # is it running
journalctl -u netbond -f        # logs
netbond-server -host YOUR_IP -print-code   # show the code again
```

To change the secret, delete `/etc/netbond/secret` and restart the service.
