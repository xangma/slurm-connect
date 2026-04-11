from __future__ import annotations

import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import threading
import time
from typing import Optional

from .models import (
    DEFAULT_SESSION_HEARTBEAT_SECONDS,
    DEFAULT_SESSION_JOB_NAME,
    DEFAULT_SESSION_STATE_DIR,
    DEFAULT_STALE_SECONDS,
    EPHEMERAL_LOCK_TTL_SECONDS,
    MONITOR_CHECK_INTERVAL,
    PERSISTENT_UNREADY_CANCEL_GRACE_SECONDS,
    PERSISTENT_UNREADY_CANCEL_POLL_SECONDS,
    SESSION_TOKEN_RE,
    TERMINAL_JOB_STATES,
    SessionInfo,
    SessionPaths,
    WorkgroupInvoker,
)


def resolve_ephemeral_lock_path(session_key: str) -> Optional[str]:
    safe_key = sanitize_token(session_key)
    if not safe_key:
        return None
    user = os.environ.get("USER") or str(os.getuid())
    base_dir = os.path.join("/tmp", f"slurm-connect-ephemeral-{user}")
    try:
        os.makedirs(base_dir, exist_ok=True)
    except Exception:
        return None
    return os.path.join(base_dir, f"{safe_key}.lock")


def is_recent_lock(path: str, now: float) -> bool:
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return False
    return (now - mtime) <= EPHEMERAL_LOCK_TTL_SECONDS


def touch_lock(path: str, now: float) -> None:
    try:
        with open(path, "a", encoding="utf-8"):
            os.utime(path, (now, now))
    except Exception:
        return


def parse_workgroup_output(output: str) -> Optional[str]:
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        match = re.match(r"\d+\s+(\S+)", line)
        if match:
            return match.group(1)
    return None


def sanitize_token(value: Optional[str]) -> str:
    trimmed = (value or "").strip()
    if not trimmed:
        return "default"
    sanitized = SESSION_TOKEN_RE.sub("-", trimmed).strip(".-")
    sanitized = sanitized or "default"
    return sanitized[:64]


def get_username(logger: logging.Logger) -> str:
    user = os.environ.get("USER")
    if user:
        return user
    try:
        return subprocess.check_output(
            ["id", "-un"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        logger.debug("Failed to resolve username.", exc_info=True)
    return "unknown"


def resolve_session_paths(
    state_dir: str, session_key: str, logger: logging.Logger
) -> SessionPaths:
    expanded_state = os.path.expanduser(
        os.path.expandvars(state_dir or DEFAULT_SESSION_STATE_DIR)
    )
    sessions_dir = os.path.join(expanded_state, "sessions")
    safe_key = sanitize_token(session_key)
    safe_user = sanitize_token(get_username(logger))
    user_dir = os.path.join(sessions_dir, safe_user, safe_key)
    legacy_dir = os.path.join(sessions_dir, safe_key)
    return SessionPaths(
        state_dir=expanded_state,
        sessions_dir=sessions_dir,
        safe_user=safe_user,
        safe_key=safe_key,
        user_dir=user_dir,
        legacy_dir=legacy_dir,
    )


def resolve_workgroup_invoker(logger: logging.Logger) -> Optional[WorkgroupInvoker]:
    path = shutil.which("workgroup")
    if path:
        return WorkgroupInvoker(mode="direct", command=path)
    try:
        result = subprocess.run(
            ["bash", "-lc", "command -v workgroup"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except Exception:
        logger.debug("Failed to probe workgroup via login shell.", exc_info=True)
        return None
    output = (result.stdout or "").strip()
    if result.returncode == 0 and output:
        return WorkgroupInvoker(mode="shell", command="workgroup")
    return None


def build_workgroup_command(invoker: WorkgroupInvoker, args: list[str]) -> list[str]:
    if invoker.mode == "direct":
        return [invoker.command, *args]
    return ["bash", "-lc", shlex.join([invoker.command, *args])]


def build_slurm_command(
    invoker: Optional[WorkgroupInvoker],
    workgroup: Optional[str],
    slurm_args: list[str],
) -> list[str]:
    if invoker:
        if not workgroup:
            raise ValueError("workgroup is required when using the workgroup wrapper")
        return build_workgroup_command(
            invoker,
            ["-g", workgroup, "--command", "@", "--", *slurm_args],
        )
    return slurm_args


def load_job_json(session_dir: str, logger: logging.Logger) -> Optional[dict]:
    path = os.path.join(session_dir, "job.json")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        logger.warning("Failed to read job.json from %s", path, exc_info=True)
        return None


def write_job_json(session_dir: str, data: dict, logger: logging.Logger) -> None:
    path = os.path.join(session_dir, "job.json")
    tmp_path = f"{path}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(data, handle)
        os.replace(tmp_path, path)
    except Exception:
        logger.error("Failed to write job.json to %s", path, exc_info=True)
        raise


class SessionLock:
    def __init__(self, path: str, logger: logging.Logger) -> None:
        self._path = path
        self._logger = logger
        self._handle = None

    def __enter__(self) -> "SessionLock":
        self._handle = open(self._path, "a+", encoding="utf-8")
        try:
            import fcntl

            fcntl.flock(self._handle, fcntl.LOCK_EX)
        except Exception:
            self._logger.debug(
                "Session lock unavailable for %s", self._path, exc_info=True
            )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is None:
            return
        try:
            import fcntl

            fcntl.flock(self._handle, fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            self._handle.close()
        except Exception:
            pass


def job_is_active(job_id: str, logger: logging.Logger) -> Optional[bool]:
    try:
        result = subprocess.run(
            ["squeue", "-h", "-j", job_id, "-o", "%i"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except Exception:
        logger.error("Failed to query squeue for job %s.", job_id, exc_info=True)
        return None
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if "Invalid job id" in stderr or "Invalid job id specified" in stderr:
            logger.info(
                "Job %s no longer valid (squeue reported invalid job id).", job_id
            )
            return False
        logger.error(
            "squeue failed for job %s (exit %s): %s",
            job_id,
            result.returncode,
            stderr,
        )
        return None
    for line in (result.stdout or "").splitlines():
        if line.strip() == job_id:
            return True
    return False


def parse_scontrol_fields(output: str) -> dict:
    fields: dict[str, str] = {}
    for token in output.replace("\n", " ").split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        fields[key] = value
    return fields


def normalize_job_state(state: str) -> str:
    normalized = (state or "").strip().upper()
    if "+" in normalized:
        normalized = normalized.split("+", 1)[0]
    return normalized


def get_job_state(job_id: str, logger: logging.Logger) -> Optional[str]:
    try:
        result = subprocess.run(
            ["squeue", "-h", "-j", job_id, "-o", "%T"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode == 0:
            for line in (result.stdout or "").splitlines():
                line = line.strip()
                if line:
                    return normalize_job_state(line)
    except Exception:
        logger.debug("Failed to query squeue job state for %s.", job_id, exc_info=True)

    try:
        result = subprocess.run(
            ["scontrol", "show", "job", "-o", job_id],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode == 0:
            fields = parse_scontrol_fields(result.stdout or "")
            state = fields.get("JobState") or fields.get("JOBSTATE")
            if state:
                return normalize_job_state(state)
    except Exception:
        logger.debug(
            "Failed to query scontrol job state for %s.", job_id, exc_info=True
        )
    return None


def wait_for_job_running(
    job_id: str,
    timeout: int,
    logger: logging.Logger,
    stop_event: Optional[threading.Event] = None,
) -> bool:
    deadline = time.time() + max(0, timeout)
    last_state: Optional[str] = None
    initial_parent = os.getppid()
    while True:
        if stop_event and stop_event.is_set():
            logger.info("Interrupted while waiting for job %s to reach RUNNING.", job_id)
            return False
        if initial_parent > 1 and os.getppid() != initial_parent:
            logger.info("Parent process exited while waiting for job %s.", job_id)
            return False
        state = get_job_state(job_id, logger)
        if state and state != last_state:
            logger.info("Job %s state: %s", job_id, state)
            last_state = state
        if state == "RUNNING":
            return True
        if state in TERMINAL_JOB_STATES:
            logger.error("Job %s entered terminal state %s.", job_id, state)
            return False
        if timeout == 0:
            return False
        if time.time() >= deadline:
            logger.error("Timed out waiting for job %s to reach RUNNING.", job_id)
            return False
        if stop_event:
            if stop_event.wait(2):
                logger.info(
                    "Interrupted while waiting for job %s to reach RUNNING.", job_id
                )
                return False
        else:
            time.sleep(2)


def get_job_nodelist(job_id: str, logger: logging.Logger) -> Optional[str]:
    try:
        result = subprocess.run(
            ["squeue", "-h", "-j", job_id, "-o", "%N"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode == 0:
            for line in (result.stdout or "").splitlines():
                line = line.strip()
                if line and line.lower() != "(null)":
                    return line
    except Exception:
        logger.debug("Failed to query squeue nodelist for %s.", job_id, exc_info=True)

    try:
        result = subprocess.run(
            ["scontrol", "show", "job", "-o", job_id],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode == 0:
            fields = parse_scontrol_fields(result.stdout or "")
            nodelist = fields.get("NodeList") or fields.get("BatchHost") or ""
            nodelist = nodelist.strip()
            if nodelist and nodelist.lower() != "(null)":
                return nodelist
    except Exception:
        logger.debug("Failed to query scontrol nodelist for %s.", job_id, exc_info=True)
    return None


def split_nodelist(nodelist: str) -> list[str]:
    parts: list[str] = []
    buf = ""
    depth = 0
    for ch in nodelist:
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            if buf:
                parts.append(buf)
            buf = ""
            continue
        buf += ch
    if buf:
        parts.append(buf)
    return parts


def expand_nodelist_fallback(nodelist: str) -> list[str]:
    results: list[str] = []
    for part in split_nodelist(nodelist):
        if "[" not in part or "]" not in part:
            results.append(part)
            continue
        prefix, rest = part.split("[", 1)
        inside, suffix = rest.split("]", 1)
        for segment in inside.split(","):
            segment = segment.strip()
            if not segment:
                continue
            if "-" in segment:
                start_str, end_str = segment.split("-", 1)
                width = max(len(start_str), len(end_str))
                try:
                    start = int(start_str)
                    end = int(end_str)
                except ValueError:
                    results.append(prefix + segment + suffix)
                    continue
                step = 1 if end >= start else -1
                for value in range(start, end + step, step):
                    results.append(prefix + str(value).zfill(width) + suffix)
            else:
                results.append(prefix + segment + suffix)
    return results


def expand_nodelist(nodelist: str, logger: logging.Logger) -> list[str]:
    if not nodelist:
        return []
    try:
        result = subprocess.run(
            ["scontrol", "show", "hostnames", nodelist],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode == 0:
            hosts = [
                line.strip()
                for line in (result.stdout or "").splitlines()
                if line.strip()
            ]
            if hosts:
                return hosts
    except Exception:
        logger.debug("Failed to expand nodelist via scontrol.", exc_info=True)
    return expand_nodelist_fallback(nodelist)


def choose_session_node(
    session_dir: str, job_id: str, logger: logging.Logger
) -> Optional[str]:
    job_data = load_job_json(session_dir, logger) or {}
    saved_node = str(job_data.get("node") or "").strip()
    nodelist = get_job_nodelist(job_id, logger)
    if not nodelist:
        return saved_node or None
    nodes = expand_nodelist(nodelist, logger)
    if not nodes:
        return saved_node or None
    chosen = saved_node if saved_node in nodes else nodes[0]
    if chosen and job_data.get("node") != chosen:
        job_data["node"] = chosen
        try:
            write_job_json(session_dir, job_data, logger)
        except Exception:
            logger.debug("Failed to update node in job.json.", exc_info=True)
    return chosen


def cleanup_stale_markers(
    session_dir: str, stale_seconds: int, logger: logging.Logger
) -> None:
    if stale_seconds <= 0:
        return
    clients_dir = os.path.join(session_dir, "clients")
    if not os.path.isdir(clients_dir):
        return
    now = time.time()
    try:
        for name in os.listdir(clients_dir):
            path = os.path.join(clients_dir, name)
            if not os.path.isfile(path):
                continue
            try:
                age = now - os.path.getmtime(path)
            except Exception:
                continue
            if age > stale_seconds:
                try:
                    os.remove(path)
                except Exception:
                    logger.debug(
                        "Failed to remove stale marker %s", path, exc_info=True
                    )
    except Exception:
        logger.debug("Failed to cleanup stale markers.", exc_info=True)


def maybe_auto_cancel_unready_persistent_job(
    invoker: Optional[WorkgroupInvoker],
    workgroup: Optional[str],
    session_info: Optional[SessionInfo],
    ready_emitted: bool,
    logger: logging.Logger,
    reason: str,
) -> bool:
    if not session_info:
        return False
    if not session_info.submitted_new:
        logger.info(
            "Skipping auto-cancel for job %s; session reused existing allocation.",
            session_info.job_id,
        )
        return False
    if ready_emitted:
        logger.info(
            "Skipping auto-cancel for job %s; connection reached ready state.",
            session_info.job_id,
        )
        return False
    clients_dir = os.path.join(session_info.session_dir, "clients")
    deadline = time.monotonic() + max(0, PERSISTENT_UNREADY_CANCEL_GRACE_SECONDS)
    active_markers = 0
    while True:
        cleanup_stale_markers(session_info.session_dir, session_info.stale_seconds, logger)
        active_markers = 0
        if os.path.isdir(clients_dir):
            try:
                active_markers = sum(
                    1
                    for name in os.listdir(clients_dir)
                    if os.path.isfile(os.path.join(clients_dir, name))
                )
            except Exception:
                logger.debug(
                    "Failed to count active markers in %s", clients_dir, exc_info=True
                )
        if active_markers > 0:
            if time.monotonic() >= deadline:
                logger.info(
                    "Skipping auto-cancel for job %s; %d marker(s) still active.",
                    session_info.job_id,
                    active_markers,
                )
                return False
            time.sleep(PERSISTENT_UNREADY_CANCEL_POLL_SECONDS)
            continue
        if time.monotonic() >= deadline:
            break
        time.sleep(PERSISTENT_UNREADY_CANCEL_POLL_SECONDS)
    is_active = job_is_active(session_info.job_id, logger)
    if is_active is False:
        logger.info("Skipping auto-cancel for job %s; job is already inactive.", session_info.job_id)
        return False
    if is_active is None:
        logger.warning(
            "Could not verify state for job %s before auto-cancel; attempting scancel.",
            session_info.job_id,
        )
    command = build_slurm_command(invoker, workgroup, ["scancel", session_info.job_id])
    logger.warning(
        "Auto-cancelling persistent job %s (%s): %s",
        session_info.job_id,
        reason,
        " ".join(command),
    )
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        logger.error(
            "Auto-cancel failed for job %s (exit %s): %s",
            session_info.job_id,
            result.returncode,
            (result.stderr or "").strip(),
        )
        return False
    logger.info("Auto-cancel succeeded for job %s.", session_info.job_id)
    return True


def extract_job_name(args: list[str]) -> Optional[str]:
    for i, arg in enumerate(args):
        if arg.startswith("--job-name="):
            return arg.split("=", 1)[1]
        if arg == "--job-name" and i + 1 < len(args):
            return args[i + 1]
        if arg.startswith("-J"):
            if arg == "-J" and i + 1 < len(args):
                return args[i + 1]
            if len(arg) > 2:
                return arg[2:]
    return None


def has_sbatch_option(args: list[str], long_opt: str, short_opt: str) -> bool:
    for i, arg in enumerate(args):
        if arg.startswith(f"{long_opt}="):
            return True
        if arg == long_opt and i + 1 < len(args):
            return True
        if arg.startswith(short_opt):
            if arg == short_opt and i + 1 < len(args):
                return True
            if len(arg) > len(short_opt):
                return True
    return False


def write_monitor_script(
    session_dir: str, idle_timeout: int, stale_seconds: int
) -> str:
    monitor_path = os.path.join(session_dir, "monitor.sh")
    session_literal = shlex.quote(session_dir)
    lines = [
        "#!/usr/bin/env bash",
        "SESSION_DIR=" + session_literal,
        'CLIENTS_DIR="${SESSION_DIR}/clients"',
        'LAST_SEEN_PATH="${SESSION_DIR}/last_seen"',
        f"IDLE_TIMEOUT={max(0, int(idle_timeout))}",
        f"STALE_SECONDS={max(0, int(stale_seconds))}",
        f"SLEEP_INTERVAL={MONITOR_CHECK_INTERVAL}",
        'idle_start=""',
        "while true; do",
        '  if [ "$IDLE_TIMEOUT" -le 0 ]; then',
        "    sleep 3600",
        "    continue",
        "  fi",
        "  now=$(date +%s)",
        "  active=0",
        '  if [ -d "$CLIENTS_DIR" ]; then',
        '    for path in "$CLIENTS_DIR"/*; do',
        '      [ -f "$path" ] || continue',
        '      if [ "$STALE_SECONDS" -gt 0 ]; then',
        '        mtime=$(stat -c %Y "$path" 2>/dev/null || stat -f %m "$path" 2>/dev/null || echo "")',
        '        if [ -n "$mtime" ]; then',
        "          age=$((now - mtime))",
        '          if [ "$age" -gt "$STALE_SECONDS" ]; then',
        '            rm -f "$path" >/dev/null 2>&1 || true',
        "            continue",
        "          fi",
        "        fi",
        "      fi",
        "      active=1",
        "      break",
        "    done",
        "  fi",
        '  if [ "$active" -eq 1 ]; then',
        '    idle_start=""',
        '    sleep "$SLEEP_INTERVAL"',
        "    continue",
        "  fi",
        '  if [ -z "$idle_start" ]; then',
        "    idle_start=$now",
        "  fi",
        "  elapsed=$((now - idle_start))",
        '  if [ "$elapsed" -ge "$IDLE_TIMEOUT" ]; then',
        '    if [ -n "$SLURM_JOB_ID" ]; then',
        '      scancel "$SLURM_JOB_ID" >/dev/null 2>&1 || true',
        "    fi",
        "    exit 0",
        "  fi",
        '  sleep "$SLEEP_INTERVAL"',
        "done",
    ]
    with open(monitor_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines) + "\n")
    try:
        os.chmod(monitor_path, 0o700)
    except Exception:
        pass
    return monitor_path


def submit_persistent_job(
    invoker: Optional[WorkgroupInvoker],
    workgroup: Optional[str],
    salloc_args: list[str],
    session_key: str,
    session_dir: str,
    idle_timeout: int,
    stale_seconds: int,
    logger: logging.Logger,
) -> SessionInfo:
    os.makedirs(session_dir, exist_ok=True)
    os.makedirs(os.path.join(session_dir, "clients"), exist_ok=True)
    monitor_script = write_monitor_script(session_dir, idle_timeout, stale_seconds)

    job_name = extract_job_name(salloc_args) or DEFAULT_SESSION_JOB_NAME

    args = ["sbatch", "--parsable"]
    if not extract_job_name(salloc_args):
        args.append(f"--job-name={job_name}")
    if not has_sbatch_option(salloc_args, "--output", "-o"):
        args.append("--output=/dev/null")
    if not has_sbatch_option(salloc_args, "--error", "-e"):
        args.append("--error=/dev/null")
    args.extend(salloc_args)
    args.append(monitor_script)
    command = build_slurm_command(invoker, workgroup, args)

    logger.info("Submitting persistent session job: %s", " ".join(command))
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        logger.critical(
            "sbatch failed (exit %s): %s",
            result.returncode,
            (result.stderr or "").strip(),
        )
        raise RuntimeError("sbatch failed")

    stdout = (result.stdout or "").strip()
    job_id = stdout.split(";", 1)[0].strip()
    if not job_id or not job_id.isdigit():
        match = re.search(r"\d+", stdout)
        job_id = match.group(0) if match else ""
    if not job_id:
        raise RuntimeError(f"Unable to parse job id from sbatch output: {stdout}")

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    safe_key = sanitize_token(session_key)
    record = {
        "job_id": job_id,
        "created_at": now,
        "args": list(salloc_args),
        "session_key": safe_key,
        "job_name": job_name,
        "idle_timeout_seconds": int(max(0, idle_timeout)),
        "stale_seconds": int(max(0, stale_seconds)),
        "heartbeat_seconds": int(DEFAULT_SESSION_HEARTBEAT_SECONDS),
    }
    write_job_json(session_dir, record, logger)
    return SessionInfo(
        session_dir=session_dir,
        job_id=job_id,
        job_name=job_name,
        idle_timeout_seconds=int(max(0, idle_timeout)),
        stale_seconds=int(max(0, stale_seconds)),
        submitted_new=True,
    )


def ensure_persistent_session(
    invoker: Optional[WorkgroupInvoker],
    workgroup: Optional[str],
    salloc_args: list[str],
    session_key: str,
    idle_timeout: int,
    state_dir: str,
    logger: logging.Logger,
) -> SessionInfo:
    paths = resolve_session_paths(state_dir, session_key, logger)
    os.makedirs(paths.sessions_dir, exist_ok=True)
    os.makedirs(os.path.join(paths.sessions_dir, paths.safe_user), exist_ok=True)
    os.makedirs(paths.user_dir, exist_ok=True)

    candidate_dirs = []
    if os.path.isfile(os.path.join(paths.user_dir, "job.json")):
        candidate_dirs.append(paths.user_dir)
    if os.path.isfile(os.path.join(paths.legacy_dir, "job.json")):
        candidate_dirs.append(paths.legacy_dir)
    if not candidate_dirs:
        candidate_dirs.append(paths.user_dir)

    lock_path = os.path.join(paths.user_dir, "lock")
    with SessionLock(lock_path, logger):
        active_candidate: Optional[SessionInfo] = None
        for candidate in candidate_dirs:
            job_data = load_job_json(candidate, logger)
            if not job_data:
                continue
            job_id = str(job_data.get("job_id") or "").strip()
            if not job_id:
                continue
            active = job_is_active(job_id, logger)
            if active is None:
                logger.warning(
                    "Unable to confirm job %s state; reusing session.", job_id
                )
                return SessionInfo(
                    session_dir=candidate,
                    job_id=job_id,
                    job_name=str(job_data.get("job_name") or ""),
                    idle_timeout_seconds=int(job_data.get("idle_timeout_seconds") or 0),
                    stale_seconds=int(
                        job_data.get("stale_seconds") or DEFAULT_STALE_SECONDS
                    ),
                    node=str(job_data.get("node") or "").strip() or None,
                )
            if active:
                active_candidate = SessionInfo(
                    session_dir=candidate,
                    job_id=job_id,
                    job_name=str(job_data.get("job_name") or ""),
                    idle_timeout_seconds=int(job_data.get("idle_timeout_seconds") or 0),
                    stale_seconds=int(
                        job_data.get("stale_seconds") or DEFAULT_STALE_SECONDS
                    ),
                    node=str(job_data.get("node") or "").strip() or None,
                )
                break

        if active_candidate:
            return active_candidate

        if candidate_dirs and candidate_dirs[0] != paths.user_dir:
            logger.info(
                "Legacy session job missing; creating new session under %s",
                paths.user_dir,
            )
        return submit_persistent_job(
            invoker=invoker,
            workgroup=workgroup,
            salloc_args=salloc_args,
            session_key=session_key,
            session_dir=paths.user_dir,
            idle_timeout=idle_timeout,
            stale_seconds=DEFAULT_STALE_SECONDS,
            logger=logger,
        )


def detect_workgroup(invoker: WorkgroupInvoker, logger: logging.Logger) -> str:
    try:
        result = subprocess.run(
            build_workgroup_command(invoker, ["-q", "workgroups"]),
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except Exception:
        logger.critical("Failed to run workgroup -q workgroups.", exc_info=True)
        raise
    if result.returncode != 0:
        logger.critical(
            "workgroup -q workgroups failed (exit %s): %s",
            result.returncode,
            (result.stderr or "").strip(),
        )
        raise RuntimeError("workgroup lookup failed")
    workgroup = parse_workgroup_output(result.stdout or "")
    if not workgroup:
        logger.critical(
            "Unable to parse workgroup from: %s", (result.stdout or "").strip()
        )
        raise RuntimeError("workgroup parse failed")
    return workgroup


def build_salloc_command(
    invoker: Optional[WorkgroupInvoker],
    workgroup: Optional[str],
    salloc_args: list[str],
    command: Optional[list[str]] = None,
    use_pty: bool = False,
) -> list[str]:
    args = ["salloc", *salloc_args]
    if command:
        step = ["srun", "--nodes=1", "--ntasks=1"]
        if use_pty:
            step.append("--pty")
        step.extend(command)
        args.extend(step)
    return build_slurm_command(invoker, workgroup, args)


def build_srun_command(
    invoker: Optional[WorkgroupInvoker],
    workgroup: Optional[str],
    job_id: str,
    shell: list[str],
    use_pty: bool,
    node: Optional[str],
) -> list[str]:
    args = ["srun", "--jobid", job_id, "--overlap", "--nodes=1", "--ntasks=1"]
    if node:
        args.extend(["--nodelist", node])
    if use_pty:
        args.append("--pty")
    args.extend(shell)
    return build_slurm_command(invoker, workgroup, args)
