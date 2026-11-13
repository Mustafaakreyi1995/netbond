# NetBond

Combine Wi-Fi, mobile data and Ethernet/USB on an Android phone into one faster,
more reliable connection, using your own server.

Unlike per-connection load balancers, NetBond bonds **every single connection**:
one download or one video stream is split across all your networks at once and
put back together on your server. If a network drops, the connections running
over it carry on over the others.

```
 apps ─► VPN interface ─► NetBond app ══ Wi-Fi ═══════╗
                                      ══ mobile data ══╬═► your server ─► internet
                                      ══ Ethernet/USB ═╝
```

## Set up

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

## What to expect

- Speed is roughly the sum of your networks, capped by your server's own
  bandwidth. Pick a server close to you; every packet makes the detour.
- Websites see your server's IP address, not your phone's.
- Mobile data is used whenever the VPN is on, including for large downloads.
- UDP (calls, games, QUIC) is tunnelled inside TCP links, so it works but gains
  no speed and can lag on a congested link.

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
