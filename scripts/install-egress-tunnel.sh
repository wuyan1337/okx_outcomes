#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

PROGRAM="${0##*/}"
NODE_ID=""
REMOTE_HOST=""
REMOTE_USER="okx-egress"
SSH_PORT=22
LOCAL_PORT=""
REGION=""
PRIVATE_KEY=""
HOST_KEY_FILE=""
HOST_KEY_FINGERPRINT=""
REPLACE=0
LOCAL_USER="okx-tunnel"

usage() {
  cat <<'EOF'
Install a persistent loopback-only SSH dynamic SOCKS tunnel on the bot host.

Usage:
  sudo bash ./scripts/install-egress-tunnel.sh \
    --node-id JP-TYO-06 \
    --host 203.0.113.20 \
    --region AU \
    --local-port 9116 \
    --private-key /root/JP-TYO-06 \
    --host-key-file /root/JP-TYO-06.host-ed25519.pub \
    --host-key-fingerprint SHA256:... \
    [--ssh-port 22] [--remote-user okx-egress] [--replace]

The host public key and SHA256 fingerprint must be obtained out-of-band from
the VPS console. The tunnel always binds to 127.0.0.1 and uses key-only SSH.
EOF
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_value() {
  local option="$1"
  local value="${2-}"
  [[ -n "$value" && "$value" != --* ]] || die "$option requires a value"
}

valid_port() {
  [[ "$1" =~ ^[0-9]+$ ]] && ((10#$1 >= 1 && 10#$1 <= 65535))
}

while (($#)); do
  case "$1" in
    --node-id)
      require_value "$1" "${2-}"; NODE_ID="$2"; shift 2 ;;
    --host)
      require_value "$1" "${2-}"; REMOTE_HOST="$2"; shift 2 ;;
    --remote-user)
      require_value "$1" "${2-}"; REMOTE_USER="$2"; shift 2 ;;
    --ssh-port)
      require_value "$1" "${2-}"; SSH_PORT="$2"; shift 2 ;;
    --local-port)
      require_value "$1" "${2-}"; LOCAL_PORT="$2"; shift 2 ;;
    --region)
      require_value "$1" "${2-}"; REGION="${2^^}"; shift 2 ;;
    --private-key)
      require_value "$1" "${2-}"; PRIVATE_KEY="$2"; shift 2 ;;
    --host-key-file)
      require_value "$1" "${2-}"; HOST_KEY_FILE="$2"; shift 2 ;;
    --host-key-fingerprint)
      require_value "$1" "${2-}"; HOST_KEY_FINGERPRINT="$2"; shift 2 ;;
    --replace)
      REPLACE=1; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      die "unknown argument: $1" ;;
  esac
done

[[ "${EUID}" -eq 0 ]] || die "run as root (sudo bash $PROGRAM ...)"
[[ "$NODE_ID" =~ ^[A-Z]{2}-[A-Z]{3}-[0-9]{2}$ ]] || \
  die "invalid --node-id; use CC-CCC-NN (for example JP-TYO-06)"
[[ "$REMOTE_HOST" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$ ]] || \
  die "--host must be an IPv4 address or DNS hostname"
[[ "$REMOTE_USER" =~ ^[a-z_][a-z0-9_-]{0,30}$ ]] || die "invalid --remote-user"
valid_port "$SSH_PORT" || die "invalid --ssh-port"
valid_port "$LOCAL_PORT" || die "invalid --local-port"
((10#$LOCAL_PORT >= 1024)) || die "--local-port must be 1024 or higher"
[[ "$REGION" == "AU" || "$REGION" == "CN" ]] || die "--region must be AU or CN"
[[ -f "$PRIVATE_KEY" && ! -L "$PRIVATE_KEY" ]] || \
  die "private key must be a regular, non-symlink file"
[[ -f "$HOST_KEY_FILE" && ! -L "$HOST_KEY_FILE" ]] || \
  die "host key must be a regular, non-symlink file"
[[ "$HOST_KEY_FINGERPRINT" =~ ^SHA256:[A-Za-z0-9+/=]{20,64}$ ]] || \
  die "invalid --host-key-fingerprint"

if [[ -r /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
else
  die "cannot identify operating system"
fi
[[ "${ID:-}" == "ubuntu" ]] || die "this installer supports Ubuntu only"

missing_packages=()
if ! command -v ssh >/dev/null 2>&1 || ! command -v ssh-keygen >/dev/null 2>&1; then
  missing_packages+=(openssh-client)
fi
command -v curl >/dev/null 2>&1 || missing_packages+=(curl)
command -v ss >/dev/null 2>&1 || missing_packages+=(iproute2)
command -v runuser >/dev/null 2>&1 || missing_packages+=(util-linux)
if ((${#missing_packages[@]})); then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends "${missing_packages[@]}"
fi

mapfile -t HOST_KEY_LINES < <(grep -Ev '^[[:space:]]*(#|$)' -- "$HOST_KEY_FILE")
[[ "${#HOST_KEY_LINES[@]}" -eq 1 ]] || die "host key file must contain exactly one key"
HOST_KEY_LINE="${HOST_KEY_LINES[0]%$'\r'}"
IFS=' ' read -r HOST_KEY_TYPE HOST_KEY_BLOB _ <<< "$HOST_KEY_LINE"
[[ "$HOST_KEY_TYPE" == "ssh-ed25519" && "$HOST_KEY_BLOB" =~ ^[A-Za-z0-9+/]+={0,3}$ ]] || \
  die "host key file must contain one ssh-ed25519 public key"

HOST_KEY_TMP="$(mktemp)"
cleanup() {
  rm -f -- "$HOST_KEY_TMP"
}
trap cleanup EXIT
printf '%s %s\n' "$HOST_KEY_TYPE" "$HOST_KEY_BLOB" > "$HOST_KEY_TMP"
ACTUAL_FINGERPRINT="$(ssh-keygen -lf "$HOST_KEY_TMP" -E sha256 | awk '{print $2}')"
[[ "$ACTUAL_FINGERPRINT" == "$HOST_KEY_FINGERPRINT" ]] || \
  die "host key fingerprint mismatch (got $ACTUAL_FINGERPRINT)"

ssh-keygen -y -P '' -f "$PRIVATE_KEY" >/dev/null 2>&1 || \
  die "private key is invalid or passphrase-protected; systemd requires a dedicated unencrypted key"

UNIT_SUFFIX="$(printf '%s' "$NODE_ID" | tr '[:upper:]' '[:lower:]')"
UNIT_NAME="okx-egress-${UNIT_SUFFIX}.service"
UNIT_FILE="/etc/systemd/system/$UNIT_NAME"
BASE_DIR="/etc/okx-egress"
KEY_FILE="$BASE_DIR/keys/$NODE_ID"
KNOWN_HOSTS_FILE="$BASE_DIR/known_hosts/$NODE_ID"
SSH_CONFIG_FILE="$BASE_DIR/config/$NODE_ID.conf"
SSH_ALIAS="okx-egress-$UNIT_SUFFIX"

if [[ -e "$UNIT_FILE" || -e "$KEY_FILE" || -e "$KNOWN_HOSTS_FILE" || -e "$SSH_CONFIG_FILE" ]]; then
  ((REPLACE == 1)) || die "node files already exist; inspect them and pass --replace to replace explicitly"
  systemctl disable --now "$UNIT_NAME" >/dev/null 2>&1 || true
fi

LISTENERS="$(ss -H -ltn "sport = :$LOCAL_PORT" 2>/dev/null || true)"
[[ -z "$LISTENERS" ]] || die "local port $LOCAL_PORT is already in use"

if ! id "$LOCAL_USER" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "/var/lib/$LOCAL_USER" \
    --user-group --shell /usr/sbin/nologin "$LOCAL_USER"
else
  EXISTING_UID="$(id -u "$LOCAL_USER")"
  EXISTING_HOME="$(getent passwd "$LOCAL_USER" | cut -d: -f6)"
  EXISTING_SHELL="$(getent passwd "$LOCAL_USER" | cut -d: -f7)"
  [[ "$EXISTING_UID" != "0" && "$EXISTING_HOME" == "/var/lib/$LOCAL_USER" && \
     "$EXISTING_SHELL" == "/usr/sbin/nologin" ]] || \
    die "existing $LOCAL_USER account is not a compatible dedicated nologin user"
fi
getent group "$LOCAL_USER" >/dev/null || die "dedicated group $LOCAL_USER is missing"
passwd -l "$LOCAL_USER" >/dev/null 2>&1 || true

install -d -m 0750 -o root -g "$LOCAL_USER" \
  "$BASE_DIR" "$BASE_DIR/keys" "$BASE_DIR/known_hosts" "$BASE_DIR/config"
install -m 0400 -o "$LOCAL_USER" -g "$LOCAL_USER" "$PRIVATE_KEY" "$KEY_FILE"
if ((10#$SSH_PORT == 22)); then
  KNOWN_HOST_PATTERN="$REMOTE_HOST"
else
  KNOWN_HOST_PATTERN="[$REMOTE_HOST]:$SSH_PORT"
fi
printf '%s %s %s\n' "$KNOWN_HOST_PATTERN" "$HOST_KEY_TYPE" "$HOST_KEY_BLOB" \
  > "$KNOWN_HOSTS_FILE"
chown root:"$LOCAL_USER" "$KNOWN_HOSTS_FILE"
chmod 0440 "$KNOWN_HOSTS_FILE"

cat > "$SSH_CONFIG_FILE" <<EOF
Host $SSH_ALIAS
    HostName $REMOTE_HOST
    Port $SSH_PORT
    User $REMOTE_USER
    IdentityFile $KEY_FILE
    IdentitiesOnly yes
    BatchMode yes
    PreferredAuthentications publickey
    PasswordAuthentication no
    KbdInteractiveAuthentication no
    StrictHostKeyChecking yes
    UserKnownHostsFile $KNOWN_HOSTS_FILE
    GlobalKnownHostsFile /dev/null
    CheckHostIP no
    HostKeyAlgorithms ssh-ed25519
    UpdateHostKeys no
    ForwardAgent no
    ForwardX11 no
    PermitLocalCommand no
    DynamicForward 127.0.0.1:$LOCAL_PORT
    ExitOnForwardFailure yes
    ServerAliveInterval 15
    ServerAliveCountMax 3
    TCPKeepAlive yes
    RequestTTY no
EOF
chown root:"$LOCAL_USER" "$SSH_CONFIG_FILE"
chmod 0440 "$SSH_CONFIG_FILE"

runuser -u "$LOCAL_USER" -- ssh -G -F "$SSH_CONFIG_FILE" "$SSH_ALIAS" >/dev/null || \
  die "generated SSH client configuration is invalid"

cat > "$UNIT_FILE" <<EOF
[Unit]
Description=OKX egress tunnel $NODE_ID
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$LOCAL_USER
Group=$LOCAL_USER
UMask=0077
ExecStart=/usr/bin/ssh -N -F $SSH_CONFIG_FILE $SSH_ALIAS
Restart=always
RestartSec=3s
TimeoutStopSec=10s
NoNewPrivileges=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectSystem=strict
ProtectHome=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictSUIDSGID=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
CapabilityBoundingSet=
AmbientCapabilities=
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6

[Install]
WantedBy=multi-user.target
EOF
chmod 0644 "$UNIT_FILE"

systemctl daemon-reload
if ! systemctl enable --now "$UNIT_NAME"; then
  systemctl disable --now "$UNIT_NAME" >/dev/null 2>&1 || true
  die "failed to start $UNIT_NAME; unit was disabled"
fi

READY=0
for _ in {1..15}; do
  if ss -H -ltn "sport = :$LOCAL_PORT" 2>/dev/null | awk '{print $4}' | \
      grep -Fxq "127.0.0.1:$LOCAL_PORT"; then
    READY=1
    break
  fi
  sleep 1
done
if ((READY == 0)); then
  systemctl disable --now "$UNIT_NAME" >/dev/null 2>&1 || true
  die "tunnel did not bind 127.0.0.1:$LOCAL_PORT; unit was disabled"
fi

BAD_BIND="$(ss -H -ltn "sport = :$LOCAL_PORT" 2>/dev/null | awk '{print $4}' | \
  grep -Fvx "127.0.0.1:$LOCAL_PORT" || true)"
if [[ -n "$BAD_BIND" ]]; then
  systemctl disable --now "$UNIT_NAME" >/dev/null 2>&1 || true
  die "unexpected non-loopback listener detected ($BAD_BIND); unit was disabled"
fi

if [[ "$REGION" == "CN" ]]; then
  CHECK_URL="https://www.okx.com/api/v5/public/time"
else
  CHECK_URL="https://us.okx.com/api/v5/public/time"
fi
if ! curl --silent --show-error --fail --noproxy '' \
    --connect-timeout 5 --max-time 15 \
    --socks5-hostname "127.0.0.1:$LOCAL_PORT" "$CHECK_URL" >/dev/null; then
  systemctl disable --now "$UNIT_NAME" >/dev/null 2>&1 || true
  die "SOCKS validation against the $REGION OKX endpoint failed; unit was disabled"
fi

printf 'Installed %s on 127.0.0.1:%s (region check: %s).\n' \
  "$NODE_ID" "$LOCAL_PORT" "$REGION"
printf 'Host key verified: %s\n' "$ACTUAL_FINGERPRINT"
printf 'Systemd unit: %s\n' "$UNIT_NAME"
printf '%s\n' 'No public SOCKS port was opened.'
printf '%s\n' 'Register the node from the Bot project with scripts/proxy_nodes.py.'
if systemctl cat okx-bot.service >/dev/null 2>&1; then
  BOT_SERVICE_USER="$(systemctl show --property=User --value okx-bot.service)"
  BOT_SERVICE_USER="${BOT_SERVICE_USER:-root}"
  printf 'IMPORTANT: You MUST run that CLI as okx-bot.service User=%s.\n' \
    "$BOT_SERVICE_USER"
  printf '%s\n' 'Running it as arbitrary root can create a root:root 0600 registry that a non-root Bot cannot read.'
  printf '  cd /opt/okx-outcomes-bot && sudo -u %q -- /opt/okx-bot-venv/bin/python scripts/proxy_nodes.py add --id %q --location CITY --port %q\n' \
    "$BOT_SERVICE_USER" "$NODE_ID" "$LOCAL_PORT"
else
  printf '%s\n' \
    'WARNING: okx-bot.service was not found, so its registry owner cannot be determined.' \
    'Do not run the registry CLI until the Bot service is installed and its effective User= is known.'
  printf 'Required arguments later: add --id %s --location CITY --port %s\n' \
    "$NODE_ID" "$LOCAL_PORT"
fi
printf '%s\n' 'See NODE_INSTALL_GUIDE.md for the okx_bot_python helper and verification steps.'
