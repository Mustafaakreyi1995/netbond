#!/usr/bin/env bash
# Installs the NetBond server on a Linux VPS and prints the code to paste
# into the phone app. Safe to run again to update.
set -euo pipefail

REPO="__REPO__"
PORT="${PORT:-443}"
BIN=/usr/local/bin/netbond-server
SECRET=/etc/netbond/secret

if [ "$(id -u)" != "0" ]; then
  echo "Please run as root (use: sudo bash install.sh)"; exit 1
fi

case "$(uname -m)" in
  x86_64|amd64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "Unsupported CPU: $(uname -m)"; exit 1 ;;
esac

systemctl stop netbond 2>/dev/null || true

# fall back to 8443 if something else already owns the port
if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${PORT}\$"; then
  echo "Port ${PORT} is already in use, using 8443 instead."
  PORT=8443
fi

echo "Downloading server (${ARCH})..."
curl -fsSL -o "${BIN}.new" "https://github.com/${REPO}/releases/latest/download/netbond-server-linux-${ARCH}"
chmod +x "${BIN}.new"
mv "${BIN}.new" "${BIN}"
mkdir -p /etc/netbond
chmod 700 /etc/netbond

cat > /etc/systemd/system/netbond.service <<UNIT
[Unit]
Description=NetBond server
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=${BIN} -listen :${PORT} -secret-file ${SECRET} -quiet
Restart=always
RestartSec=2
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
UNIT

# BBR copes much better with lossy mobile links than the default
cat > /etc/sysctl.d/90-netbond.conf <<SYSCTL
net.core.default_qdisc=fq
net.ipv4.tcp_congestion_control=bbr
SYSCTL
sysctl --system >/dev/null 2>&1 || true

if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q "Status: active"; then
  ufw allow "${PORT}/tcp" >/dev/null || true
fi
if command -v firewall-cmd >/dev/null 2>&1 && firewall-cmd --state >/dev/null 2>&1; then
  firewall-cmd --permanent --add-port="${PORT}/tcp" >/dev/null || true
  firewall-cmd --reload >/dev/null || true
fi

systemctl daemon-reload
systemctl enable --now netbond >/dev/null 2>&1
sleep 1

IP="$(curl -4 -fsS --max-time 8 https://api.ipify.org 2>/dev/null || true)"
[ -n "${IP}" ] || IP="$(hostname -I 2>/dev/null | awk '{print $1}')"

echo
if systemctl is-active --quiet netbond; then
  echo "NetBond server is running on port ${PORT}."
else
  echo "The service did not start. Check: journalctl -u netbond -n 30"
fi
echo
echo "Paste this code into the NetBond app (keep it private):"
echo
"${BIN}" -listen ":${PORT}" -secret-file "${SECRET}" -host "${IP}" -print-code
echo
echo "If your provider has a cloud firewall, allow TCP port ${PORT} there too."
