#!/usr/bin/env bash
# Switch the running container's APT sources to a configurable mirror.
#
# Designed to run as root inside a Debian/Ubuntu-based container BEFORE any
# `apt-get update` / `apt-get install` step. The rocm/primus runtime image
# used by BenchP2P is Ubuntu 24.04 (noble), and on some clusters Canonical's
# default archive.ubuntu.com / security.ubuntu.com are unreachable, so
# mooncake/dependencies.sh -y or similar install steps fail until apt is
# pointed at a reachable mirror.
#
# Handles both source formats:
#   - deb822 /etc/apt/sources.list.d/ubuntu.sources   (Ubuntu >= 24.04)
#   - legacy /etc/apt/sources.list                    (Ubuntu <= 22.04 / Debian)
#
# The first run backs up the original to *.benchp2p.bak; subsequent runs
# rewrite from that backup so the script is idempotent and reversible
# (`--restore` puts it back).
#
# Usage:
#   bash scripts/switch_apt_mirror.sh [options]
#
# Options:
#   --mirror URL            Mirror base for main archive (no trailing /).
#                           Example: https://mirrors.tuna.tsinghua.edu.cn/ubuntu
#   --security-mirror URL   Override security suite URI (default: same as --mirror).
#   --preset NAME           Convenience aliases for --mirror. Known names:
#                             tuna       https://mirrors.tuna.tsinghua.edu.cn/ubuntu
#                             aliyun     https://mirrors.aliyun.com/ubuntu
#                             ustc       https://mirrors.ustc.edu.cn/ubuntu
#                             huawei     https://mirrors.huaweicloud.com/repository/ubuntu
#                             163        https://mirrors.163.com/ubuntu
#                             oracle-iad https://ubuntu-archive.objectstorage.us-ashburn-1.oraclecloud.com/ubuntu
#                             oracle-phx https://ubuntu-archive.objectstorage.us-phoenix-1.oraclecloud.com/ubuntu
#                             oracle-fra https://ubuntu-archive.objectstorage.eu-frankfurt-1.oraclecloud.com/ubuntu
#                             default    http://archive.ubuntu.com/ubuntu (restore upstream)
#   --no-update             Skip the trailing `apt-get update`.
#   --restore               Restore the backed-up original and exit.
#   --dry-run               Print what would be written without modifying files.
#   -h, --help              Show this help.
#
# Exit codes:
#   0 success / nothing to do, 1 usage error, 2 not running as root,
#   3 no recognised sources file found.

set -euo pipefail

PRESET=""
MIRROR=""
SECURITY_MIRROR=""
DO_UPDATE=1
RESTORE=0
DRY_RUN=0

log() { printf '[switch_apt_mirror] %s\n' "$*" >&2; }
die() { printf '[switch_apt_mirror] ERROR: %s\n' "$*" >&2; exit "${2:-1}"; }

usage() {
  sed -n '/^# Usage:/,/^# Exit codes:/p' "$0" | sed 's/^# \?//' | sed '$d'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mirror) MIRROR="$2"; shift 2 ;;
    --security-mirror) SECURITY_MIRROR="$2"; shift 2 ;;
    --preset) PRESET="$2"; shift 2 ;;
    --no-update) DO_UPDATE=0; shift ;;
    --restore) RESTORE=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1 (use --help)" ;;
  esac
done

case "${PRESET}" in
  "")          : ;;
  tuna)        MIRROR="${MIRROR:-https://mirrors.tuna.tsinghua.edu.cn/ubuntu}" ;;
  aliyun)      MIRROR="${MIRROR:-https://mirrors.aliyun.com/ubuntu}" ;;
  ustc)        MIRROR="${MIRROR:-https://mirrors.ustc.edu.cn/ubuntu}" ;;
  huawei)      MIRROR="${MIRROR:-https://mirrors.huaweicloud.com/repository/ubuntu}" ;;
  163)         MIRROR="${MIRROR:-https://mirrors.163.com/ubuntu}" ;;
  oracle-iad)  MIRROR="${MIRROR:-https://ubuntu-archive.objectstorage.us-ashburn-1.oraclecloud.com/ubuntu}" ;;
  oracle-phx)  MIRROR="${MIRROR:-https://ubuntu-archive.objectstorage.us-phoenix-1.oraclecloud.com/ubuntu}" ;;
  oracle-fra)  MIRROR="${MIRROR:-https://ubuntu-archive.objectstorage.eu-frankfurt-1.oraclecloud.com/ubuntu}" ;;
  default)
    MIRROR="${MIRROR:-http://archive.ubuntu.com/ubuntu}"
    SECURITY_MIRROR="${SECURITY_MIRROR:-http://security.ubuntu.com/ubuntu}"
    ;;
  *) die "unknown preset: ${PRESET}" ;;
esac

if [[ "${RESTORE}" == "0" && -z "${MIRROR}" ]]; then
  die "no mirror specified; use --mirror URL or --preset NAME (or --restore)"
fi

# Default security to same host as main; for the upstream "default" preset
# we set them separately above to preserve security.ubuntu.com.
SECURITY_MIRROR="${SECURITY_MIRROR:-${MIRROR}}"
MIRROR="${MIRROR%/}"
SECURITY_MIRROR="${SECURITY_MIRROR%/}"

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  die "must run as root (we modify /etc/apt). Re-run with sudo or as root inside the container." 2
fi

LEGACY=/etc/apt/sources.list
DEB822=/etc/apt/sources.list.d/ubuntu.sources

backup_once() {
  local f="$1"
  if [[ -f "${f}" && ! -f "${f}.benchp2p.bak" ]]; then
    if [[ "${DRY_RUN}" == "1" ]]; then
      log "would back up ${f} -> ${f}.benchp2p.bak"
    else
      cp -a "${f}" "${f}.benchp2p.bak"
      log "backed up ${f} -> ${f}.benchp2p.bak"
    fi
  fi
}

restore_one() {
  local f="$1"
  if [[ -f "${f}.benchp2p.bak" ]]; then
    if [[ "${DRY_RUN}" == "1" ]]; then
      log "would restore ${f} from ${f}.benchp2p.bak"
    else
      cp -a "${f}.benchp2p.bak" "${f}"
      log "restored ${f}"
    fi
  fi
}

if [[ "${RESTORE}" == "1" ]]; then
  restore_one "${LEGACY}"
  restore_one "${DEB822}"
  if [[ "${DO_UPDATE}" == "1" && "${DRY_RUN}" != "1" ]]; then
    log "running apt-get update"
    apt-get update -y || true
  fi
  exit 0
fi

write_file() {
  local f="$1" content="$2"
  if [[ "${DRY_RUN}" == "1" ]]; then
    log "would write ${f}:"
    printf '%s\n' "${content}" | sed 's/^/    /' >&2
  else
    printf '%s\n' "${content}" > "${f}"
    chmod 0644 "${f}"
    log "wrote ${f}"
  fi
}

# Detect Ubuntu/Debian codename. /etc/os-release is the canonical source on
# both distros; UBUNTU_CODENAME wins on Ubuntu, VERSION_CODENAME on Debian.
codename=""
if [[ -f /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  codename="${UBUNTU_CODENAME:-${VERSION_CODENAME:-}}"
fi

rewrote_anything=0

# deb822 layout (preferred when present)
if [[ -f "${DEB822}" ]]; then
  backup_once "${DEB822}"
  src="${DEB822}.benchp2p.bak"
  [[ -f "${src}" ]] || src="${DEB822}"
  cn="${codename}"
  if [[ -z "${cn}" ]]; then
    cn="$(awk '/^Suites:/ {print $2; exit}' "${src}" | sed 's/-.*//')"
  fi
  [[ -n "${cn}" ]] || die "could not determine codename for ${DEB822}"
  content="# Rewritten by switch_apt_mirror.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)
# Original backed up at ${DEB822}.benchp2p.bak
Types: deb
URIs: ${MIRROR}/
Suites: ${cn} ${cn}-updates ${cn}-backports
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg

Types: deb
URIs: ${SECURITY_MIRROR}/
Suites: ${cn}-security
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg"
  write_file "${DEB822}" "${content}"
  rewrote_anything=1
fi

# Legacy flat sources.list. Only rewrite if it actually has active deb lines
# (Ubuntu 24.04 ships an empty / comment-only stub alongside ubuntu.sources).
if [[ -f "${LEGACY}" ]] && grep -qE '^[[:space:]]*deb[[:space:]]' "${LEGACY}"; then
  backup_once "${LEGACY}"
  src="${LEGACY}.benchp2p.bak"
  [[ -f "${src}" ]] || src="${LEGACY}"
  cn="${codename}"
  if [[ -z "${cn}" ]]; then
    cn="$(awk '$1=="deb" {print $3; exit}' "${src}" | sed 's/-.*//')"
  fi
  [[ -n "${cn}" ]] || die "could not determine codename for ${LEGACY}"
  content="# Rewritten by switch_apt_mirror.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)
# Original backed up at ${LEGACY}.benchp2p.bak
deb ${MIRROR} ${cn} main restricted universe multiverse
deb ${MIRROR} ${cn}-updates main restricted universe multiverse
deb ${MIRROR} ${cn}-backports main restricted universe multiverse
deb ${SECURITY_MIRROR} ${cn}-security main restricted universe multiverse"
  write_file "${LEGACY}" "${content}"
  rewrote_anything=1
fi

if [[ "${rewrote_anything}" == "0" ]]; then
  die "no recognised sources file found (looked at ${DEB822} and ${LEGACY})" 3
fi

if [[ "${DO_UPDATE}" == "1" && "${DRY_RUN}" != "1" ]]; then
  log "running apt-get update"
  apt-get update -y
fi

log "done; main = ${MIRROR}, security = ${SECURITY_MIRROR}"
