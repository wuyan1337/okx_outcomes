#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

PROGRAM="${0##*/}"
EGRESS_USER="okx-egress"
NODE_ID=""
BOT_CIDR=""
PUBLIC_KEY_FILE=""

usage() {
  cat <<'EOF'
Bootstrap an Ubuntu VPS as a restricted SSH egress node.

No SOCKS service is installed and no port 9111 is opened. The bot server must
create a loopback-only SSH dynamic forward with install-egress-tunnel.sh.

Usage:
  sudo bash ./scripts/bootstrap-egress-node.sh \
    --node-id JP-TYO-06 \
    --bot-cidr 203.0.113.10/32 \
    --public-key-file /root/JP-TYO-06.pub \
    [--user okx-egress]

Required:
  --node-id ID             CC-CCC-NN style canonical node ID.
  --bot-cidr CIDR          Single bot source IP as IPv4 /32 or IPv6 /128.
  --public-key-file PATH   Out-of-band ed25519 public key from the bot server.

Optional:
  --user USER              Dedicated remote account (default: okx-egress).
  -h, --help               Show this help.
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

while (($#)); do
  case "$1" in
    --node-id)
      require_value "$1" "${2-}"
      NODE_ID="$2"
      shift 2
      ;;
    --bot-cidr)
      require_value "$1" "${2-}"
      BOT_CIDR="$2"
      shift 2
      ;;
    --public-key-file)
      require_value "$1" "${2-}"
      PUBLIC_KEY_FILE="$2"
      shift 2
      ;;
    --user)
      require_value "$1" "${2-}"
      EGRESS_USER="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[[ "${EUID}" -eq 0 ]] || die "run as root (sudo bash $PROGRAM ...)"
[[ -n "$NODE_ID" ]] || die "--node-id is required"
[[ "$NODE_ID" =~ ^[A-Z]{2}-[A-Z]{3}-[0-9]{2}$ ]] || \
  die "invalid --node-id; use CC-CCC-NN (for example JP-TYO-06)"
[[ -n "$BOT_CIDR" ]] || die "--bot-cidr is required"
[[ -n "$PUBLIC_KEY_FILE" ]] || die "--public-key-file is required"
[[ "$EGRESS_USER" =~ ^[a-z_][a-z0-9_-]{0,30}$ ]] || die "invalid --user"
[[ -f "$PUBLIC_KEY_FILE" && ! -L "$PUBLIC_KEY_FILE" ]] || \
  die "public key must be a regular, non-symlink file"

if [[ -r /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
else
  die "cannot identify operating system"
fi
[[ "${ID:-}" == "ubuntu" ]] || die "this bootstrap supports Ubuntu only"

command -v python3 >/dev/null 2>&1 || die "python3 is required for CIDR validation"
BOT_TEST_IP="$(python3 - "$BOT_CIDR" <<'PY'
import ipaddress
import sys

network = ipaddress.ip_network(sys.argv[1], strict=False)
if (
    network.is_unspecified
    or network.is_multicast
    or network.prefixlen != network.max_prefixlen
):
    raise SystemExit(1)
print(network.network_address)
PY
)" || die "invalid --bot-cidr"

missing_packages=()
command -v sshd >/dev/null 2>&1 || missing_packages+=(openssh-server)
command -v ssh-keygen >/dev/null 2>&1 || missing_packages+=(openssh-client)
command -v ss >/dev/null 2>&1 || missing_packages+=(iproute2)
if ((${#missing_packages[@]})); then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends "${missing_packages[@]}"
fi
ssh-keygen -A >/dev/null
[[ -f /etc/ssh/ssh_host_ed25519_key.pub ]] || die "ed25519 SSH host key is unavailable"

mapfile -t KEY_LINES < <(grep -Ev '^[[:space:]]*(#|$)' -- "$PUBLIC_KEY_FILE")
[[ "${#KEY_LINES[@]}" -eq 1 ]] || die "public key file must contain exactly one key"
PUBLIC_KEY="${KEY_LINES[0]%$'\r'}"
[[ "$PUBLIC_KEY" =~ ^(ssh-ed25519|sk-ssh-ed25519@openssh.com)[[:space:]]+[A-Za-z0-9+/]+={0,3}([[:space:]].*)?$ ]] || \
  die "only an ed25519 OpenSSH public key is accepted"
ssh-keygen -lf "$PUBLIC_KEY_FILE" -E sha256 >/dev/null 2>&1 || die "invalid public key"

assert_no_public_9111() {
  local listeners bad
  listeners="$(ss -H -ltn 'sport = :9111' 2>/dev/null || true)"
  [[ -z "$listeners" ]] && return 0
  bad="$(printf '%s\n' "$listeners" | awk '{print $4}' | \
    grep -Ev '^(127\.0\.0\.1:9111|\[::1\]:9111)$' || true)"
  [[ -z "$bad" ]] || die "public/non-loopback port 9111 is already listening: $bad"
}

assert_no_public_9111

if ! id "$EGRESS_USER" >/dev/null 2>&1; then
  useradd --create-home --home-dir "/var/lib/$EGRESS_USER" \
    --user-group --shell /usr/sbin/nologin "$EGRESS_USER"
else
  EXISTING_UID="$(id -u "$EGRESS_USER")"
  EXISTING_HOME="$(getent passwd "$EGRESS_USER" | cut -d: -f6)"
  EXISTING_SHELL="$(getent passwd "$EGRESS_USER" | cut -d: -f7)"
  [[ "$EXISTING_UID" != "0" && "$EXISTING_HOME" == "/var/lib/$EGRESS_USER" && \
     "$EXISTING_SHELL" == "/usr/sbin/nologin" ]] || \
    die "existing $EGRESS_USER account is not a compatible dedicated nologin user"
fi
getent group "$EGRESS_USER" >/dev/null || die "dedicated group $EGRESS_USER is missing"
passwd -l "$EGRESS_USER" >/dev/null 2>&1 || true

EGRESS_HOME="$(getent passwd "$EGRESS_USER" | cut -d: -f6)"
[[ -n "$EGRESS_HOME" && "$EGRESS_HOME" == /* ]] || die "cannot resolve egress user home"
install -d -m 0700 -o "$EGRESS_USER" -g "$EGRESS_USER" "$EGRESS_HOME/.ssh"

AUTHORIZED_KEYS="$EGRESS_HOME/.ssh/authorized_keys"
SSHD_FRAGMENT="/etc/ssh/sshd_config.d/90-okx-egress.conf"
AUTHORIZED_KEYS_TMP="$(mktemp)"
AUTHORIZED_KEYS_BACKUP_TMP="$(mktemp)"
SSHD_FRAGMENT_TMP="$(mktemp)"
SSHD_BACKUP_TMP="$(mktemp)"
HAD_OLD_AUTHORIZED_KEYS=0
HAD_OLD_FRAGMENT=0
AUTHORIZED_KEYS_CHANGED=0
SSHD_FRAGMENT_CHANGED=0
SETUP_COMMITTED=0

cleanup() {
  local status=$?
  trap - EXIT
  if ((SETUP_COMMITTED == 0)); then
    if ((AUTHORIZED_KEYS_CHANGED)); then
      if ((HAD_OLD_AUTHORIZED_KEYS)); then
        cp -a -- "$AUTHORIZED_KEYS_BACKUP_TMP" "$AUTHORIZED_KEYS" || true
      else
        rm -f -- "$AUTHORIZED_KEYS"
      fi
    fi
    if ((SSHD_FRAGMENT_CHANGED)); then
      if ((HAD_OLD_FRAGMENT)); then
        cp -a -- "$SSHD_BACKUP_TMP" "$SSHD_FRAGMENT" || true
      else
        rm -f -- "$SSHD_FRAGMENT"
      fi
    fi
  fi
  rm -f -- "$AUTHORIZED_KEYS_TMP" "$AUTHORIZED_KEYS_BACKUP_TMP" \
    "$SSHD_FRAGMENT_TMP" "$SSHD_BACKUP_TMP"
  exit "$status"
}
trap cleanup EXIT

[[ ! -L "$AUTHORIZED_KEYS" ]] || die "refusing symlink authorized_keys: $AUTHORIZED_KEYS"
if [[ -f "$AUTHORIZED_KEYS" ]]; then
  cp -a -- "$AUTHORIZED_KEYS" "$AUTHORIZED_KEYS_BACKUP_TMP"
  HAD_OLD_AUTHORIZED_KEYS=1
elif [[ -e "$AUTHORIZED_KEYS" ]]; then
  die "authorized_keys exists but is not a regular file"
fi
[[ ! -L "$SSHD_FRAGMENT" ]] || die "refusing symlink sshd fragment: $SSHD_FRAGMENT"
if [[ -f "$SSHD_FRAGMENT" ]]; then
  cp -a -- "$SSHD_FRAGMENT" "$SSHD_BACKUP_TMP"
  HAD_OLD_FRAGMENT=1
elif [[ -e "$SSHD_FRAGMENT" ]]; then
  die "sshd fragment exists but is not a regular file"
fi

printf 'from="%s",restrict,port-forwarding %s\n' "$BOT_CIDR" "$PUBLIC_KEY" > "$AUTHORIZED_KEYS_TMP"
install -m 0600 -o "$EGRESS_USER" -g "$EGRESS_USER" \
  "$AUTHORIZED_KEYS_TMP" "$AUTHORIZED_KEYS"
AUTHORIZED_KEYS_CHANGED=1

cat > "$SSHD_FRAGMENT_TMP" <<EOF
# Managed by $PROGRAM for $NODE_ID. Do not expose a public SOCKS listener.
Match User $EGRESS_USER
    AuthenticationMethods publickey
    PubkeyAuthentication yes
    PasswordAuthentication no
    KbdInteractiveAuthentication no
    AllowTcpForwarding local
    GatewayPorts no
    AllowAgentForwarding no
    X11Forwarding no
    PermitTTY no
    PermitTunnel no
    MaxSessions 0
    PermitOpen us.okx.com:443 www.okx.com:443 wsus.okx.com:443 ws.okx.com:8443

Match all
EOF

install -d -m 0755 /etc/ssh/sshd_config.d
install -m 0644 -o root -g root "$SSHD_FRAGMENT_TMP" "$SSHD_FRAGMENT"
SSHD_FRAGMENT_CHANGED=1

restore_sshd_fragment() {
  if ((HAD_OLD_FRAGMENT)); then
    cp -a -- "$SSHD_BACKUP_TMP" "$SSHD_FRAGMENT"
  else
    rm -f -- "$SSHD_FRAGMENT"
  fi
}

if ! sshd -t -f /etc/ssh/sshd_config; then
  restore_sshd_fragment
  die "sshd rejected the generated configuration; previous fragment restored"
fi

EFFECTIVE_SSHD="$(
  sshd -T -f /etc/ssh/sshd_config \
    -C "user=$EGRESS_USER,host=localhost,addr=$BOT_TEST_IP"
)" || {
  restore_sshd_fragment
  die "could not evaluate the generated SSH Match configuration"
}
effective_sshd_value() {
  local key="$1"
  printf '%s\n' "$EFFECTIVE_SSHD" | awk -v key="$key" '$1 == key {$1=""; sub(/^ /, ""); print; exit}'
}
[[ "$(effective_sshd_value authenticationmethods)" == "publickey" ]] || {
  restore_sshd_fragment; die "effective SSH authentication is not public-key-only";
}
[[ "$(effective_sshd_value passwordauthentication)" == "no" ]] || {
  restore_sshd_fragment; die "effective SSH password authentication is not disabled";
}
[[ "$(effective_sshd_value kbdinteractiveauthentication)" == "no" ]] || {
  restore_sshd_fragment; die "effective SSH keyboard authentication is not disabled";
}
[[ "$(effective_sshd_value allowtcpforwarding)" == "local" ]] || {
  restore_sshd_fragment; die "effective SSH forwarding is not restricted to local forwarding";
}
[[ "$(effective_sshd_value gatewayports)" == "no" ]] || {
  restore_sshd_fragment; die "effective SSH GatewayPorts is not disabled";
}
[[ "$(effective_sshd_value permittty)" == "no" ]] || {
  restore_sshd_fragment; die "effective SSH PTY access is not disabled";
}
[[ "$(effective_sshd_value maxsessions)" == "0" ]] || {
  restore_sshd_fragment; die "effective SSH session channels are not disabled";
}
EFFECTIVE_PERMIT_OPEN="$(effective_sshd_value permitopen)"
for destination in \
  us.okx.com:443 www.okx.com:443 wsus.okx.com:443 ws.okx.com:8443; do
  [[ " $EFFECTIVE_PERMIT_OPEN " == *" $destination "* ]] || {
    restore_sshd_fragment; die "effective SSH PermitOpen is missing $destination";
  }
done

reload_ok=0
if systemctl is-active --quiet ssh.service; then
  systemctl reload ssh.service && reload_ok=1
elif systemctl is-active --quiet sshd.service; then
  systemctl reload sshd.service && reload_ok=1
fi
if ((reload_ok == 0)); then
  restore_sshd_fragment
  sshd -t -f /etc/ssh/sshd_config || true
  die "SSH service is not active or could not reload; previous fragment restored"
fi

assert_no_public_9111
SETUP_COMMITTED=1

FINGERPRINT="$(ssh-keygen -lf "$PUBLIC_KEY_FILE" -E sha256 | awk '{print $2}')"
printf 'Configured egress node %s.\n' "$NODE_ID"
printf 'Authorized bot CIDR: %s\n' "$BOT_CIDR"
printf 'Tunnel key fingerprint: %s\n' "$FINGERPRINT"
printf '%s\n' 'No SOCKS daemon was installed; port 9111 remains closed.'
printf '%s\n' 'Also restrict the cloud security group SSH rule to the bot CIDR.'
printf '%s\n' 'From the VPS console, record this host-key fingerprint for tunnel installation:'
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub -E sha256
