#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/diagnose-proxy-flow.sh [options]

Purpose:
  Diagnose Slurm Connect proxy behavior outside VS Code Remote-SSH by running:
  1) local logic-only proxy checks (no cluster required), and
  2) optional remote checks over plain ssh to a login host.

Options:
  --ssh-target TARGET          SSH target for remote checks (for example: user@login4.example.edu).
  --identity-file PATH         Optional SSH identity file for remote checks.
  --ssh-config PATH            Optional SSH config file for remote checks.
  --partition NAME             Optional Slurm partition for remote checks.
  --remote-proxy-path PATH     Remote path to proxy script. Default: ~/.slurm-connect/vscode-proxy.py
  --upload-proxy-from PATH     Upload this local proxy script to remote /tmp and test that file.
  --session-key KEY            Session key used for two-run lock test.
  --skip-local                 Skip local logic-only checks.
  --skip-remote-proxy          Skip remote proxy two-run checks.
  --skip-remote-salloc         Skip remote Slurm host-placement sanity checks.
  -h, --help                   Show help.

Examples:
  scripts/diagnose-proxy-flow.sh
  scripts/diagnose-proxy-flow.sh --ssh-target moricex@login4.sciama.icg.port.ac.uk --partition sciama3.q
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: required command not found: $cmd" >&2
    exit 1
  fi
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

sanitize_token() {
  local value
  value="$(trim "$1")"
  if [[ -z "$value" ]]; then
    printf 'default'
    return
  fi
  local sanitized
  sanitized="$(printf '%s' "$value" | sed -E 's/[^A-Za-z0-9_.-]+/-/g; s/^[.-]+//; s/[.-]+$//')"
  if [[ -z "$sanitized" ]]; then
    sanitized="default"
  fi
  printf '%s' "${sanitized:0:64}"
}

SSH_TARGET=""
IDENTITY_FILE=""
SSH_CONFIG=""
PARTITION=""
REMOTE_PROXY_PATH="~/.slurm-connect/vscode-proxy.py"
UPLOAD_PROXY_FROM=""
SESSION_KEY="diag-$(date +%s)-$$"
SKIP_LOCAL=0
SKIP_REMOTE_PROXY=0
SKIP_REMOTE_SALLOC=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ssh-target)
      SSH_TARGET="${2:-}"
      shift 2
      ;;
    --identity-file)
      IDENTITY_FILE="${2:-}"
      shift 2
      ;;
    --ssh-config)
      SSH_CONFIG="${2:-}"
      shift 2
      ;;
    --partition)
      PARTITION="${2:-}"
      shift 2
      ;;
    --remote-proxy-path)
      REMOTE_PROXY_PATH="${2:-}"
      shift 2
      ;;
    --upload-proxy-from)
      UPLOAD_PROXY_FROM="${2:-}"
      shift 2
      ;;
    --session-key)
      SESSION_KEY="${2:-}"
      shift 2
      ;;
    --skip-local)
      SKIP_LOCAL=1
      shift
      ;;
    --skip-remote-proxy)
      SKIP_REMOTE_PROXY=1
      shift
      ;;
    --skip-remote-salloc)
      SKIP_REMOTE_SALLOC=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_cmd ssh
require_cmd sed
require_cmd mktemp

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_PROXY_PATH="$ROOT_DIR/media/vscode-proxy.py"
INSTALLED_PROXY_PATH="$(ls -1dt "$HOME"/.vscode-insiders/extensions/xangma.slurm-connect-*/media/vscode-proxy.py 2>/dev/null | head -n1 || true)"

if [[ $SKIP_LOCAL -eq 0 ]] && [[ ! -f "$LOCAL_PROXY_PATH" ]]; then
  echo "error: local proxy script not found: $LOCAL_PROXY_PATH" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d -t slurm-connect-diag-XXXXXX)"

summary_line() {
  printf '[%s] %s\n' "$1" "$2"
}

PROXY_ARGS=()
build_proxy_common_args() {
  local session="$1"
  local partition_name="$2"
  PROXY_ARGS=(
    -vv
    --session-mode=ephemeral
    "--session-key=$session"
    --salloc-arg=--immediate=5
    --salloc-arg=--nodes=1
    --salloc-arg=--ntasks-per-node=1
    --salloc-arg=--cpus-per-task=1
    --salloc-arg=--time=00:02:00
  )
  if [[ -n "$partition_name" ]]; then
    PROXY_ARGS+=("--salloc-arg=--partition=$partition_name")
  fi
}

LOCAL_LOG1="$TMP_DIR/local-proxy-run1.log"
LOCAL_LOG2="$TMP_DIR/local-proxy-run2.log"
REMOTE_PROXY_OUT="$TMP_DIR/remote-proxy.out"
REMOTE_SALLOC_OUT="$TMP_DIR/remote-salloc.out"

if [[ $SKIP_LOCAL -eq 0 ]]; then
  require_cmd python3
  LOCAL_FEATURE="legacy-bypass"
  if grep -q "ephemeral_exec_server_bypass" "$LOCAL_PROXY_PATH"; then
    LOCAL_FEATURE="gated-bypass"
  fi
  LOCAL_SHA="unknown"
  if command -v shasum >/dev/null 2>&1; then
    LOCAL_SHA="$(shasum -a 256 "$LOCAL_PROXY_PATH" | awk '{print $1}')"
  fi
  INSTALLED_FEATURE="unknown"
  INSTALLED_SHA="unknown"
  if [[ -n "$INSTALLED_PROXY_PATH" ]] && [[ -f "$INSTALLED_PROXY_PATH" ]]; then
    INSTALLED_FEATURE="legacy-bypass"
    if grep -q "ephemeral_exec_server_bypass" "$INSTALLED_PROXY_PATH"; then
      INSTALLED_FEATURE="gated-bypass"
    fi
    if command -v shasum >/dev/null 2>&1; then
      INSTALLED_SHA="$(shasum -a 256 "$INSTALLED_PROXY_PATH" | awk '{print $1}')"
    fi
  fi
  SAFE_KEY="$(sanitize_token "$SESSION_KEY")"
  LOCAL_USER="${USER:-$(id -u)}"
  LOCAL_LOCK="/tmp/slurm-connect-ephemeral-${LOCAL_USER}/${SAFE_KEY}.lock"
  rm -f "$LOCAL_LOCK"

  build_proxy_common_args "$SESSION_KEY" "$PARTITION"

  python3 "$LOCAL_PROXY_PATH" "${PROXY_ARGS[@]}" --log-file "$LOCAL_LOG1" < /dev/null >/dev/null 2>&1 || true
  sleep 1
  python3 "$LOCAL_PROXY_PATH" "${PROXY_ARGS[@]}" --log-file "$LOCAL_LOG2" < /dev/null >/dev/null 2>&1 || true

  LOCAL_BYPASS="no"
  LOCAL_LOGIN_SHELL="no"
  if grep -q "Recent ephemeral lock detected" "$LOCAL_LOG2" 2>/dev/null; then
    LOCAL_BYPASS="yes"
  fi
  if grep -q "Launching remote shell: /bin/bash -l" "$LOCAL_LOG2" 2>/dev/null; then
    LOCAL_LOGIN_SHELL="yes"
  fi

  summary_line "local" "session_key=$SESSION_KEY"
  summary_line "local" "proxy_feature_mode=$LOCAL_FEATURE"
  summary_line "local" "proxy_sha256=$LOCAL_SHA"
  summary_line "local" "installed_proxy_path=${INSTALLED_PROXY_PATH:-not-found}"
  summary_line "local" "installed_proxy_feature_mode=$INSTALLED_FEATURE"
  summary_line "local" "installed_proxy_sha256=$INSTALLED_SHA"
  summary_line "local" "second_run_ephemeral_bypass_detected=$LOCAL_BYPASS"
  summary_line "local" "second_run_login_shell_selected=$LOCAL_LOGIN_SHELL"
  summary_line "local" "log1=$LOCAL_LOG1"
  summary_line "local" "log2=$LOCAL_LOG2"
fi

SSH_ARGS=(
  -T
  -o BatchMode=yes
  -o ConnectTimeout=20
  -o StrictHostKeyChecking=accept-new
  -o RemoteCommand=none
  -o RequestTTY=no
)
if [[ -n "$IDENTITY_FILE" ]]; then
  SSH_ARGS+=(-i "$IDENTITY_FILE")
fi
if [[ -n "$SSH_CONFIG" ]]; then
  SSH_ARGS+=(-F "$SSH_CONFIG")
fi

if [[ -n "$SSH_TARGET" ]] && [[ $SKIP_REMOTE_PROXY -eq 0 ]]; then
  if [[ -n "$UPLOAD_PROXY_FROM" ]]; then
    if [[ ! -f "$UPLOAD_PROXY_FROM" ]]; then
      echo "error: upload source proxy file not found: $UPLOAD_PROXY_FROM" >&2
      exit 1
    fi
    require_cmd scp
    SAFE_UPLOAD_KEY="$(sanitize_token "$SESSION_KEY")"
    REMOTE_PROXY_PATH="/tmp/slurm-connect-proxy-upload-${SAFE_UPLOAD_KEY}.py"
    SCP_ARGS=(
      -o BatchMode=yes
      -o ConnectTimeout=20
      -o StrictHostKeyChecking=accept-new
      -o RemoteCommand=none
      -o RequestTTY=no
    )
    if [[ -n "$IDENTITY_FILE" ]]; then
      SCP_ARGS+=(-i "$IDENTITY_FILE")
    fi
    if [[ -n "$SSH_CONFIG" ]]; then
      SCP_ARGS+=(-F "$SSH_CONFIG")
    fi
    UPLOAD_PROXY_DIR="$(cd "$(dirname "$UPLOAD_PROXY_FROM")" && pwd)"
    UPLOAD_PROXY_PACKAGE_FROM="$UPLOAD_PROXY_DIR/vscode_proxy"
    if grep -q 'from vscode_proxy import' "$UPLOAD_PROXY_FROM" && [[ ! -d "$UPLOAD_PROXY_PACKAGE_FROM" ]]; then
      echo "error: upload source proxy package not found: $UPLOAD_PROXY_PACKAGE_FROM" >&2
      exit 1
    fi
    REMOTE_PROXY_DIR="$(dirname "$REMOTE_PROXY_PATH")"
    ssh "${SSH_ARGS[@]}" "$SSH_TARGET" "mkdir -p $REMOTE_PROXY_DIR" >/dev/null
    scp "${SCP_ARGS[@]}" "$UPLOAD_PROXY_FROM" "$SSH_TARGET:$REMOTE_PROXY_PATH" >/dev/null
    if [[ -d "$UPLOAD_PROXY_PACKAGE_FROM" ]]; then
      scp -r "${SCP_ARGS[@]}" "$UPLOAD_PROXY_PACKAGE_FROM" "$SSH_TARGET:$REMOTE_PROXY_DIR/" >/dev/null
    fi
    summary_line "remote-proxy" "uploaded_proxy_from=$UPLOAD_PROXY_FROM"
    summary_line "remote-proxy" "uploaded_proxy_to=$REMOTE_PROXY_PATH"
  fi

  ssh "${SSH_ARGS[@]}" "$SSH_TARGET" "bash -s" -- "$REMOTE_PROXY_PATH" "$SESSION_KEY" "$PARTITION" >"$REMOTE_PROXY_OUT" 2>&1 <<'REMOTE_PROXY'
set -u
proxy_path="$1"
session_key="$2"
partition="${3:-}"

trim_local() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

sanitize_local() {
  local value
  value="$(trim_local "$1")"
  if [ -z "$value" ]; then
    printf 'default'
    return
  fi
  local sanitized
  sanitized="$(printf '%s' "$value" | sed -E 's/[^A-Za-z0-9_.-]+/-/g; s/^[.-]+//; s/[.-]+$//')"
  if [ -z "$sanitized" ]; then
    sanitized="default"
  fi
  printf '%s' "${sanitized:0:64}"
}

py_bin="$(command -v python3 || command -v python || true)"
if [ -z "$py_bin" ]; then
  echo "__REMOTE_PROXY_ERROR__ python_not_found"
  exit 0
fi

proxy_expanded="${proxy_path/#\~/$HOME}"
if [ ! -f "$proxy_expanded" ]; then
  echo "__REMOTE_PROXY_ERROR__ proxy_not_found:$proxy_expanded"
  exit 0
fi

proxy_feature="legacy-bypass"
if grep -q "ephemeral_exec_server_bypass" "$proxy_expanded"; then
  proxy_feature="gated-bypass"
fi
proxy_sha="unknown"
if command -v sha256sum >/dev/null 2>&1; then
  proxy_sha="$(sha256sum "$proxy_expanded" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  proxy_sha="$(shasum -a 256 "$proxy_expanded" | awk '{print $1}')"
fi
echo "__REMOTE_PROXY_FEATURE__ $proxy_feature"
echo "__REMOTE_PROXY_SHA256__ $proxy_sha"

safe_key="$(sanitize_local "$session_key")"
remote_user="${USER:-$(id -u)}"
lock_path="/tmp/slurm-connect-ephemeral-${remote_user}/${safe_key}.lock"
log1="/tmp/slurm-connect-proxy-diag-${safe_key}-run1.log"
log2="/tmp/slurm-connect-proxy-diag-${safe_key}-run2.log"
rm -f "$lock_path" "$log1" "$log2"

cmd=(
  "$py_bin" "$proxy_expanded" -vv --session-mode=ephemeral "--session-key=$session_key"
  --salloc-arg=--immediate=5
  --salloc-arg=--nodes=1
  --salloc-arg=--ntasks-per-node=1
  --salloc-arg=--cpus-per-task=1
  --salloc-arg=--time=00:02:00
)
if [ -n "$partition" ]; then
  cmd+=("--salloc-arg=--partition=$partition")
fi

"${cmd[@]}" --log-file "$log1" < /dev/null >/dev/null 2>&1 || true
sleep 1
"${cmd[@]}" --log-file "$log2" < /dev/null >/dev/null 2>&1 || true

echo "__REMOTE_PROXY_LOG1__ $log1"
sed -n '1,180p' "$log1" 2>/dev/null || true
echo "__REMOTE_PROXY_LOG2__ $log2"
sed -n '1,180p' "$log2" 2>/dev/null || true

if grep -q "Recent ephemeral lock detected" "$log2" 2>/dev/null; then
  echo "__REMOTE_PROXY_BYPASS__ yes"
else
  echo "__REMOTE_PROXY_BYPASS__ no"
fi
if grep -q "Launching remote shell: /bin/bash -l" "$log2" 2>/dev/null; then
  echo "__REMOTE_PROXY_LOGIN_SHELL__ yes"
else
  echo "__REMOTE_PROXY_LOGIN_SHELL__ no"
fi
REMOTE_PROXY

  REMOTE_BYPASS="$(grep -E '^__REMOTE_PROXY_BYPASS__ ' "$REMOTE_PROXY_OUT" | tail -n1 | awk '{print $2}' || true)"
  REMOTE_LOGIN_SHELL="$(grep -E '^__REMOTE_PROXY_LOGIN_SHELL__ ' "$REMOTE_PROXY_OUT" | tail -n1 | awk '{print $2}' || true)"
  REMOTE_PROXY_ERROR="$(grep -E '^__REMOTE_PROXY_ERROR__ ' "$REMOTE_PROXY_OUT" | tail -n1 | cut -d' ' -f2- || true)"
  REMOTE_PROXY_FEATURE="$(grep -E '^__REMOTE_PROXY_FEATURE__ ' "$REMOTE_PROXY_OUT" | tail -n1 | awk '{print $2}' || true)"
  REMOTE_PROXY_SHA="$(grep -E '^__REMOTE_PROXY_SHA256__ ' "$REMOTE_PROXY_OUT" | tail -n1 | awk '{print $2}' || true)"

  summary_line "remote-proxy" "target=$SSH_TARGET"
  if [[ -n "$REMOTE_PROXY_ERROR" ]]; then
    summary_line "remote-proxy" "error=$REMOTE_PROXY_ERROR"
  else
    summary_line "remote-proxy" "proxy_feature_mode=${REMOTE_PROXY_FEATURE:-unknown}"
    summary_line "remote-proxy" "proxy_sha256=${REMOTE_PROXY_SHA:-unknown}"
    summary_line "remote-proxy" "second_run_ephemeral_bypass_detected=${REMOTE_BYPASS:-unknown}"
    summary_line "remote-proxy" "second_run_login_shell_selected=${REMOTE_LOGIN_SHELL:-unknown}"
  fi
  summary_line "remote-proxy" "output=$REMOTE_PROXY_OUT"
elif [[ -z "$SSH_TARGET" ]] && [[ $SKIP_REMOTE_PROXY -eq 0 ]]; then
  summary_line "remote-proxy" "skipped (no --ssh-target provided)"
fi

if [[ -n "$SSH_TARGET" ]] && [[ $SKIP_REMOTE_SALLOC -eq 0 ]]; then
  ssh "${SSH_ARGS[@]}" "$SSH_TARGET" "bash -s" -- "$PARTITION" >"$REMOTE_SALLOC_OUT" 2>&1 <<'REMOTE_SALLOC'
set -u
partition="${1:-}"
salloc_cmd=(
  salloc
  --immediate=5
  --nodes=1
  --ntasks-per-node=1
  --cpus-per-task=1
  --time=00:02:00
)
if [ -n "$partition" ]; then
  salloc_cmd+=("--partition=$partition")
fi
salloc_cmd+=(
  bash -lc
  'echo "__DIRECT_SALLOC_HOST__ $(hostname -f)"; echo "__DIRECT_SALLOC_JOB__ ${SLURM_JOB_ID:-none}"; echo "__DIRECT_SALLOC_NODELIST__ ${SLURM_JOB_NODELIST:-none}"'
)

if "${salloc_cmd[@]}" < /dev/null; then
  echo "__DIRECT_SALLOC_STATUS__ ok"
else
  status=$?
  echo "__DIRECT_SALLOC_STATUS__ fail:$status"
fi

salloc_srun_cmd=(
  salloc
  --immediate=5
  --nodes=1
  --ntasks-per-node=1
  --cpus-per-task=1
  --time=00:02:00
)
if [ -n "$partition" ]; then
  salloc_srun_cmd+=("--partition=$partition")
fi
salloc_srun_cmd+=(
  srun
  --nodes=1
  --ntasks=1
  bash -lc
  'echo "__SALLOC_SRUN_HOST__ $(hostname -f)"; echo "__SALLOC_SRUN_JOB__ ${SLURM_JOB_ID:-none}"; echo "__SALLOC_SRUN_NODELIST__ ${SLURM_JOB_NODELIST:-none}"'
)

if "${salloc_srun_cmd[@]}" < /dev/null; then
  echo "__SALLOC_SRUN_STATUS__ ok"
else
  status=$?
  echo "__SALLOC_SRUN_STATUS__ fail:$status"
fi

srun_cmd=(
  srun
  --immediate=5
  --nodes=1
  --ntasks=1
  --cpus-per-task=1
  --time=00:02:00
)
if [ -n "$partition" ]; then
  srun_cmd+=("--partition=$partition")
fi
srun_cmd+=(
  bash -lc
  'echo "__DIRECT_SRUN_HOST__ $(hostname -f)"; echo "__DIRECT_SRUN_JOB__ ${SLURM_JOB_ID:-none}"'
)

if "${srun_cmd[@]}" < /dev/null; then
  echo "__DIRECT_SRUN_STATUS__ ok"
else
  status=$?
  echo "__DIRECT_SRUN_STATUS__ fail:$status"
fi
REMOTE_SALLOC

  DIRECT_SALLOC_STATUS="$(grep -E '^__DIRECT_SALLOC_STATUS__ ' "$REMOTE_SALLOC_OUT" | tail -n1 | awk '{print $2}' || true)"
  DIRECT_SALLOC_HOST="$(grep -E '^__DIRECT_SALLOC_HOST__ ' "$REMOTE_SALLOC_OUT" | tail -n1 | cut -d' ' -f2- || true)"
  SALLOC_SRUN_STATUS="$(grep -E '^__SALLOC_SRUN_STATUS__ ' "$REMOTE_SALLOC_OUT" | tail -n1 | awk '{print $2}' || true)"
  SALLOC_SRUN_HOST="$(grep -E '^__SALLOC_SRUN_HOST__ ' "$REMOTE_SALLOC_OUT" | tail -n1 | cut -d' ' -f2- || true)"
  DIRECT_STATUS="$(grep -E '^__DIRECT_SRUN_STATUS__ ' "$REMOTE_SALLOC_OUT" | tail -n1 | awk '{print $2}' || true)"
  DIRECT_HOST="$(grep -E '^__DIRECT_SRUN_HOST__ ' "$REMOTE_SALLOC_OUT" | tail -n1 | cut -d' ' -f2- || true)"
  summary_line "remote-salloc" "target=$SSH_TARGET"
  summary_line "remote-salloc" "salloc_shell_status=${DIRECT_SALLOC_STATUS:-unknown}"
  summary_line "remote-salloc" "salloc_shell_host=${DIRECT_SALLOC_HOST:-unknown}"
  summary_line "remote-salloc" "salloc_plus_srun_status=${SALLOC_SRUN_STATUS:-unknown}"
  summary_line "remote-salloc" "salloc_plus_srun_host=${SALLOC_SRUN_HOST:-unknown}"
  summary_line "remote-salloc" "direct_srun_status=${DIRECT_STATUS:-unknown}"
  summary_line "remote-salloc" "direct_srun_host=${DIRECT_HOST:-unknown}"
  summary_line "remote-salloc" "output=$REMOTE_SALLOC_OUT"
elif [[ -z "$SSH_TARGET" ]] && [[ $SKIP_REMOTE_SALLOC -eq 0 ]]; then
  summary_line "remote-salloc" "skipped (no --ssh-target provided)"
fi

summary_line "done" "tmp_dir=$TMP_DIR"
summary_line "done" "Logs are preserved in the tmp_dir."
