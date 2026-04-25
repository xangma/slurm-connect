#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STATE_DIR="${SLURM_FIXTURE_STATE_DIR:-${ROOT_DIR}/.e2e/slurm-fixture}"
UPSTREAM_DIR="${STATE_DIR}/slurm-docker-cluster"
SSH_DIR="${STATE_DIR}/ssh"
PRIVATE_KEY="${SSH_DIR}/id_ed25519"
PUBLIC_KEY="${PRIVATE_KEY}.pub"
AUTHORIZED_KEYS="${SSH_DIR}/authorized_keys"
KNOWN_HOSTS="${SSH_DIR}/known_hosts"
SSH_CONFIG="${SSH_DIR}/ssh_config"

UPSTREAM_URL="https://github.com/giovtorres/slurm-docker-cluster.git"
UPSTREAM_REF="${SLURM_FIXTURE_UPSTREAM_REF:-4903b3419b3f6fff6101456a696f2c8395eef7e8}"
SLURM_VERSION="${SLURM_FIXTURE_SLURM_VERSION:-25.11.4}"
COMPOSE_PROJECT_NAME="${SLURM_FIXTURE_COMPOSE_PROJECT:-slurmconnecte2e}"
SSH_ALIAS="${SLURM_FIXTURE_SSH_ALIAS:-slurm-e2e}"
SSH_HOST="${SLURM_FIXTURE_SSH_HOST:-127.0.0.1}"
SSH_PORT="${SLURM_FIXTURE_SSH_PORT:-3022}"
CPU_WORKER_COUNT="${SLURM_FIXTURE_CPU_WORKER_COUNT:-2}"
FIXTURE_USER="${SLURM_FIXTURE_USER:-slurmconnect}"
FIXTURE_UID="${SLURM_FIXTURE_UID:-2001}"
FIXTURE_GID="${SLURM_FIXTURE_GID:-2001}"
FIXTURE_GROUP="${SLURM_FIXTURE_GROUP:-slurmconnect}"
FIXTURE_HOME="${SLURM_FIXTURE_HOME:-/data/home/${FIXTURE_USER}}"

LOCAL_IMAGE="slurm-docker-cluster:${SLURM_VERSION}"
REMOTE_IMAGE="giovtorres/slurm-docker-cluster:${SLURM_VERSION}"

log() {
  printf '[slurm-fixture] %s\n' "$*"
}

die() {
  printf '[slurm-fixture] ERROR: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

compose() {
  (
    cd "${UPSTREAM_DIR}"
    docker compose "$@"
  )
}

ssh_exec() {
  ssh -F "${SSH_CONFIG}" "${SSH_ALIAS}" "$@"
}

clone_upstream() {
  mkdir -p "${STATE_DIR}"
  if [[ ! -d "${UPSTREAM_DIR}/.git" ]]; then
    log "Cloning upstream fixture repo into ${UPSTREAM_DIR}"
    git clone "${UPSTREAM_URL}" "${UPSTREAM_DIR}"
  fi

  (
    cd "${UPSTREAM_DIR}"
    git fetch --depth 1 origin "${UPSTREAM_REF}"
    git checkout --force "${UPSTREAM_REF}"
  )
}

ensure_keypair() {
  mkdir -p "${SSH_DIR}"
  if [[ ! -f "${PRIVATE_KEY}" ]]; then
    log "Generating disposable SSH keypair in ${SSH_DIR}"
    ssh-keygen -t ed25519 -q -N "" -C "slurm-connect-e2e" -f "${PRIVATE_KEY}" >/dev/null
  fi
  cp "${PUBLIC_KEY}" "${AUTHORIZED_KEYS}"
  chmod 600 "${AUTHORIZED_KEYS}"
  touch "${KNOWN_HOSTS}"
  chmod 600 "${KNOWN_HOSTS}"
}

write_env_file() {
  cat > "${UPSTREAM_DIR}/.env" <<EOF
COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME}
SLURM_VERSION=${SLURM_VERSION}
MYSQL_USER=slurm
MYSQL_PASSWORD=password
MYSQL_DATABASE=slurm_acct_db
SSH_ENABLE=true
SSH_AUTHORIZED_KEYS=${AUTHORIZED_KEYS}
SSH_PORT=${SSH_PORT}
CPU_WORKER_COUNT=${CPU_WORKER_COUNT}
EOF
}

write_ssh_config() {
  cat > "${SSH_CONFIG}" <<EOF
Host ${SSH_ALIAS}
  HostName ${SSH_HOST}
  User ${FIXTURE_USER}
  Port ${SSH_PORT}
  IdentityFile ${PRIVATE_KEY}
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
  UserKnownHostsFile ${KNOWN_HOSTS}
  LogLevel ERROR

Host ${SSH_HOST}
  HostName ${SSH_HOST}
  User ${FIXTURE_USER}
  Port ${SSH_PORT}
  IdentityFile ${PRIVATE_KEY}
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
  UserKnownHostsFile ${KNOWN_HOSTS}
  LogLevel ERROR
EOF
  chmod 600 "${SSH_CONFIG}"
}

ensure_image() {
  if docker image inspect "${LOCAL_IMAGE}" >/dev/null 2>&1; then
    return
  fi

  log "Local image ${LOCAL_IMAGE} not found, trying Docker Hub"
  if docker pull "${REMOTE_IMAGE}" >/dev/null 2>&1; then
    docker tag "${REMOTE_IMAGE}" "${LOCAL_IMAGE}"
    return
  fi

  log "Prebuilt image unavailable, building locally"
  compose build
}

refresh_known_hosts() {
  ssh-keygen -R "[${SSH_HOST}]:${SSH_PORT}" -f "${KNOWN_HOSTS}" >/dev/null 2>&1 || true
  local attempts=30
  for ((i = 0; i < attempts; i += 1)); do
    if ssh-keyscan -p "${SSH_PORT}" "${SSH_HOST}" >> "${KNOWN_HOSTS}" 2>/dev/null; then
      return
    fi
    sleep 1
  done
  die "Unable to collect SSH host key from ${SSH_HOST}:${SSH_PORT}"
}

wait_for_cluster() {
  log "Waiting for Slurm controller to become ready"
  local attempts=90
  for ((i = 0; i < attempts; i += 1)); do
    if docker exec slurmctld bash -lc 'sinfo -h -o "%P|%D|%t"' >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done
  compose ps >&2 || true
  die "Slurm fixture did not become ready in time"
}

ensure_fixture_user_in_container() {
  local container="$1"
  docker exec "${container}" bash -lc "
    set -euo pipefail
    if ! getent group '${FIXTURE_GROUP}' >/dev/null 2>&1; then
      groupadd -g '${FIXTURE_GID}' '${FIXTURE_GROUP}'
    fi
    if ! id -u '${FIXTURE_USER}' >/dev/null 2>&1; then
      if [ -d '${FIXTURE_HOME}' ]; then
        useradd -M -d '${FIXTURE_HOME}' -s /bin/bash -u '${FIXTURE_UID}' -g '${FIXTURE_GID}' '${FIXTURE_USER}'
      else
        useradd -m -d '${FIXTURE_HOME}' -s /bin/bash -u '${FIXTURE_UID}' -g '${FIXTURE_GID}' '${FIXTURE_USER}'
      fi
    fi
    mkdir -p '${FIXTURE_HOME}/.ssh' '${FIXTURE_HOME}/.slurm-connect'
    chown -R '${FIXTURE_UID}:${FIXTURE_GID}' '${FIXTURE_HOME}'
    chmod 700 '${FIXTURE_HOME}' '${FIXTURE_HOME}/.ssh' '${FIXTURE_HOME}/.slurm-connect'
  "
}

ensure_fixture_user() {
  log "Creating disposable non-root fixture user ${FIXTURE_USER}"
  ensure_fixture_user_in_container slurmctld
  for ((i = 1; i <= CPU_WORKER_COUNT; i += 1)); do
    ensure_fixture_user_in_container "${COMPOSE_PROJECT_NAME}-cpu-worker-${i}"
  done
  docker exec slurmctld bash -lc "
    set -euo pipefail
    install -m 600 -o '${FIXTURE_UID}' -g '${FIXTURE_GID}' /tmp/authorized_keys_host '${FIXTURE_HOME}/.ssh/authorized_keys'
  "
}

install_proxy_script() {
  log "Installing local vscode-proxy.py into the fixture login node"
  ssh_exec 'mkdir -p ~/.slurm-connect'
  scp -F "${SSH_CONFIG}" "${ROOT_DIR}/media/vscode-proxy.py" "${SSH_ALIAS}:~/.slurm-connect/vscode-proxy.py" >/dev/null
  scp -r -F "${SSH_CONFIG}" "${ROOT_DIR}/media/vscode_proxy" "${SSH_ALIAS}:~/.slurm-connect/" >/dev/null
  ssh_exec 'chmod 700 ~/.slurm-connect/vscode-proxy.py'
}

print_connection_info() {
  cat <<EOF
Fixture ready.

SSH alias: ${SSH_ALIAS}
SSH config: ${SSH_CONFIG}
Private key: ${PRIVATE_KEY}
Known hosts: ${KNOWN_HOSTS}
Login host: ${SSH_HOST}:${SSH_PORT}

Suggested Slurm Connect settings:
{
  "slurmConnect.loginHosts": ["${SSH_HOST}"],
  "slurmConnect.user": "${FIXTURE_USER}",
  "slurmConnect.identityFile": "${PRIVATE_KEY}",
  "slurmConnect.additionalSshOptions": {
    "Port": "${SSH_PORT}",
    "UserKnownHostsFile": "${KNOWN_HOSTS}"
  },
  "slurmConnect.sshQueryConfigPath": "${SSH_CONFIG}",
  "slurmConnect.sshHostKeyChecking": "accept-new"
}
EOF
}

run_cluster_sanity() {
  log "Running targeted cluster sanity checks"

  local controller_ping
  controller_ping="$(docker exec slurmctld scontrol ping)"
  [[ "${controller_ping}" == *"is UP"* ]] || die "slurmctld did not respond to scontrol ping"

  local node_count=0
  local attempts=30
  for ((i = 0; i < attempts; i += 1)); do
    node_count="$(docker exec slurmctld bash -lc 'sinfo -h -o "%D" | awk "{sum += \$1} END {print sum+0}"')"
    if [[ "${node_count}" =~ ^[0-9]+$ ]] && (( node_count >= CPU_WORKER_COUNT )); then
      break
    fi
    sleep 2
  done
  [[ "${node_count}" =~ ^[0-9]+$ ]] || die "Unable to determine cluster node count"
  (( node_count >= CPU_WORKER_COUNT )) || die "Expected at least ${CPU_WORKER_COUNT} registered compute nodes, found ${node_count}"
}

run_ssh_smoke() {
  log "Running SSH + Slurm smoke checks through the exposed login node"

  local remote_user
  remote_user="$(ssh_exec 'whoami' | tail -n 1 | tr -d '\r')"
  [[ "${remote_user}" == "${FIXTURE_USER}" ]] || die "Unexpected SSH login user: ${remote_user}"
  log "SSH login user: ${remote_user}"

  local remote_python
  remote_python="$(ssh_exec 'python3 --version')"
  log "Remote Python: ${remote_python}"

  local partitions
  partitions="$(ssh_exec 'sinfo -h -o "%P|%D|%t"')"
  [[ -n "${partitions}" ]] || die "sinfo returned no partition data over SSH"
  printf '%s\n' "${partitions}"

  local interactive_host
  interactive_host="$(ssh_exec 'srun --nodes=1 --ntasks=1 hostname' | tail -n 1 | tr -d '\r')"
  [[ "${interactive_host}" =~ ^c[0-9]+$ ]] || die "Unexpected srun hostname: ${interactive_host}"
  log "srun landed on ${interactive_host}"

  local allocation_host
  allocation_host="$(ssh_exec 'salloc --quiet --nodes=1 --ntasks=1 --time=00:01:00 srun hostname' | tail -n 1 | tr -d '\r')"
  [[ "${allocation_host}" =~ ^c[0-9]+$ ]] || die "Unexpected salloc/srun hostname: ${allocation_host}"
  log "salloc+srun landed on ${allocation_host}"

  local job_id
  job_id="$(ssh_exec "sbatch --parsable --output='${FIXTURE_HOME}/slurm-connect-e2e-%j.out' --wrap='hostname'")"
  job_id="$(printf '%s' "${job_id}" | tail -n 1 | tr -d '\r')"
  [[ "${job_id}" =~ ^[0-9]+$ ]] || die "Unexpected sbatch job id: ${job_id}"
  log "Submitted sbatch job ${job_id}"

  local attempts=60
  local output=""
  for ((i = 0; i < attempts; i += 1)); do
    if output="$(ssh_exec "cat '${FIXTURE_HOME}/slurm-connect-e2e-${job_id}.out'" 2>/dev/null)"; then
      output="$(printf '%s' "${output}" | tail -n 1 | tr -d '\r')"
      if [[ "${output}" =~ ^c[0-9]+$ ]]; then
        log "sbatch completed on ${output}"
        break
      fi
    fi
    sleep 2
  done
  [[ "${output}" =~ ^c[0-9]+$ ]] || die "sbatch output for job ${job_id} did not appear in time"

  local proxy_help
  proxy_help="$(ssh_exec 'python3 ~/.slurm-connect/vscode-proxy.py --help | head -n 1')"
  [[ "${proxy_help}" == usage:* ]] || die "Proxy script help output was unexpected: ${proxy_help}"
}

prepare() {
  require_cmd git
  require_cmd docker
  require_cmd ssh
  require_cmd ssh-keygen
  require_cmd ssh-keyscan
  require_cmd scp

  clone_upstream
  ensure_keypair
  write_env_file
  write_ssh_config
}

up() {
  prepare
  ensure_image
  log "Starting Slurm fixture"
  compose up -d
  wait_for_cluster
  ensure_fixture_user
  refresh_known_hosts
  print_connection_info
}

down() {
  if [[ -d "${UPSTREAM_DIR}" ]]; then
    log "Stopping Slurm fixture"
    compose down
  fi
}

clean() {
  if [[ -d "${UPSTREAM_DIR}" ]]; then
    log "Removing Slurm fixture containers and volumes"
    compose down -v
  fi
}

status() {
  prepare
  compose ps
  echo
  if docker ps --format '{{.Names}}' | grep -qx 'slurmctld'; then
    docker exec slurmctld bash -lc 'sinfo -h -o "%P|%D|%t"'
  else
    log "slurmctld is not running"
  fi
}

shell() {
  prepare
  exec docker exec -it slurmctld bash
}

ssh_shell() {
  prepare
  exec ssh -F "${SSH_CONFIG}" "${SSH_ALIAS}"
}

smoke() {
  up
  run_cluster_sanity
  install_proxy_script
  run_ssh_smoke
}

install_proxy() {
  prepare
  install_proxy_script
}

settings() {
  prepare
  print_connection_info
}

usage() {
  cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  prepare   Clone/pin upstream fixture repo and generate disposable SSH keys
  up        Start the fixture and print connection details
  down      Stop fixture containers
  clean     Stop fixture containers and remove fixture volumes
  status    Show docker compose status and sinfo output
  shell     Open a shell inside slurmctld
  ssh       Open an SSH shell to the exposed login node
  smoke     Run targeted readiness checks plus real SSH/Slurm smoke tests
  install-proxy  Install the local proxy package into a running fixture
  settings  Print the generated connection info and suggested extension settings
EOF
}

main() {
  local cmd="${1:-}"
  case "${cmd}" in
    prepare) prepare ;;
    up) up ;;
    down) down ;;
    clean) clean ;;
    status) status ;;
    shell) shell ;;
    ssh) ssh_shell ;;
    smoke) smoke ;;
    install-proxy) install_proxy ;;
    settings) settings ;;
    *) usage; [[ -n "${cmd}" ]] && exit 1 ;;
  esac
}

main "$@"
