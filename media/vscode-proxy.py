#!/usr/bin/env python3
"""
Slurm Connect VS Code Remote-SSH stdio+TCP proxy.

Intended usage: configured as an SSH RemoteCommand on the login node. The proxy
launches an interactive Slurm shell on a compute node, passes VS Code stdio
through, and rewrites the "listeningOn=" line to a local TCP proxy port.
Supports persistent sessions (sbatch allocation + srun job steps) when enabled.
Uses the workgroup wrapper when available; otherwise runs Slurm commands directly.
Do not print banners to stdout; VS Code parses stdout.
Requires Python 3.9+.
"""

from __future__ import annotations

import argparse
import asyncio
import errno
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Optional, Tuple

MARKER_HOST_PREFIX = "HOSTNAME="
DEFAULT_SESSION_STATE_DIR = "~/.slurm-connect"
DEFAULT_STALE_SECONDS = 90
DEFAULT_SESSION_HEARTBEAT_SECONDS = 30
DEFAULT_SESSION_JOB_NAME = "slurm-connect"
CLIENT_TOUCH_INTERVAL = 30.0
MONITOR_CHECK_INTERVAL = 15
DEFAULT_LOG_FILE_TEMPLATE = ""
TERMINAL_JOB_STATES = {
    "CANCELLED",
    "COMPLETED",
    "FAILED",
    "TIMEOUT",
    "NODE_FAIL",
    "PREEMPTED",
    "BOOT_FAIL",
    "OUT_OF_MEMORY",
}

LEGACY_LISTENING_ON_RE = re.compile(
    r"^([^0-9]*listeningOn=[^0-9]*)(([0-9][0-9]*\.){3}[0-9][0-9]*:)?([0-9][0-9]*)([^0-9]*)$"
)
LEGACY_LISTENING_ON_SEARCH_RE = re.compile(
    r"([^0-9]*listeningOn=[^0-9]*)(([0-9][0-9]*\.){3}[0-9][0-9]*:)?([0-9][0-9]*)([^0-9]*)"
)
LISTENING_ON_RE = re.compile(r"listeningOn=(?P<wrap>==)?(?P<addr>[^\s=]+)(?(wrap)==)")
HOST_127_RE = re.compile(r"--host=(?P<quote>[\"']?)127\.0\.0\.1(?P=quote)")
COMMAND_SHELL_RE = re.compile(r"\bcommand-shell\b")
ON_HOST_RE = re.compile(r"--on-host=(?P<quote>[\"']?)(?P<value>[^\s\"']+)(?P=quote)")
LISTEN_ARGS_RE = re.compile(r'LISTEN_ARGS=(?P<quote>["\'])(?P<body>.*?)(?P=quote)')
LEGACY_HOST_REWRITE_RE = re.compile(
    r"((\$args =.*)|(\$VSCH_SERVER_SCRIPT.*))--host=127\.0\.0\.1"
)
LEGACY_LISTEN_ARGS_RE = re.compile(
    r'(LISTEN_ARGS=".*)(--on-host=(([0-9][0-9]*\.){3}[0-9][0-9]*))'
)
LEGACY_CLI_CMD_RE = re.compile(
    r"(VSCODE_CLI_REQUIRE_TOKEN=[0-9a-fA-F-]*.*\$CLI_PATH.*command-shell )(.*)(--on-host=(([0-9][0-9]*\.){3}[0-9][0-9]*))"
)
HOST_REWRITE_HINT_RE = re.compile(
    r"(\$args\b|\$VSCH_SERVER_SCRIPT|\bVSCH_SERVER_SCRIPT\b|\bVSCODE\b|"
    r"\bSERVER_SCRIPT\b|\bcommand-shell\b|\bLISTEN_ARGS\b|--on-host=)",
    re.IGNORECASE,
)
SESSION_TOKEN_RE = re.compile(r"[^A-Za-z0-9_.-]+")
HOSTNAME_VALUE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
EPHEMERAL_LOCK_TTL_SECONDS = 15


@dataclass(frozen=True)
class ListeningInfo:
    port: int
    had_host: bool
    host: Optional[str]
    addr_span: Tuple[int, int]


@dataclass
class StdinRewriteState:
    warned_unmatched_host: bool = False
    listen_args_rewritten: bool = False
    warned_listen_args_unparsed: bool = False


@dataclass
class SharedState:
    target_host: Optional[str] = None
    target_port: Optional[int] = None
    proxy_port: Optional[int] = None
    proxy_ready: bool = False
    proxy_failed: bool = False
    proxy_start_requested: bool = False
    shutdown: bool = False
    listening_seen: bool = False
    listening_handled: bool = False


@dataclass
class SessionPaths:
    state_dir: str
    sessions_dir: str
    safe_user: str
    safe_key: str
    user_dir: str
    legacy_dir: str


@dataclass
class SessionInfo:
    session_dir: str
    job_id: str
    job_name: str
    idle_timeout_seconds: int
    stale_seconds: int
    node: Optional[str] = None


@dataclass(frozen=True)
class WorkgroupInvoker:
    mode: str  # "direct" or "shell"
    command: str


class TeeStream:
    def __init__(self, stream, tee_file=None) -> None:
        self._stream = stream
        self._tee_file = tee_file
        self._lock = threading.Lock()

    def write(self, data: str) -> None:
        with self._lock:
            self._stream.write(data)
            self._stream.flush()
            if self._tee_file is not None:
                self._tee_file.write(data)
                self._tee_file.flush()

    def flush(self) -> None:
        with self._lock:
            self._stream.flush()
            if self._tee_file is not None:
                self._tee_file.flush()


def parse_hostname_marker(line: str, marker: str) -> Optional[str]:
    index = line.find(marker)
    if index < 0:
        return None
    payload = line[index + len(marker) :]
    host_index = payload.find(MARKER_HOST_PREFIX)
    if host_index < 0:
        return None
    raw = payload[host_index + len(MARKER_HOST_PREFIX) :].strip()
    if not raw:
        return None
    token = raw.split()[0].strip("\"'")
    if not token or not HOSTNAME_VALUE_RE.match(token):
        return None
    return token


def parse_listening_on_line(line: str) -> Optional[ListeningInfo]:
    legacy_match = LEGACY_LISTENING_ON_RE.match(line)
    legacy_offset = 0
    if not legacy_match:
        legacy_match = LEGACY_LISTENING_ON_SEARCH_RE.search(line)
        if legacy_match:
            legacy_offset = legacy_match.start()
    if legacy_match:
        port = int(legacy_match.group(4))
        had_host = bool(legacy_match.group(2))
        host = None
        if had_host:
            host = legacy_match.group(2)
            if host.endswith(":"):
                host = host[:-1]
        if had_host:
            start = legacy_match.start(2)
            end = legacy_match.end(4)
        else:
            start = legacy_match.start(4)
            end = legacy_match.end(4)
        return ListeningInfo(
            port=port,
            had_host=had_host,
            host=host,
            addr_span=(start + legacy_offset, end + legacy_offset),
        )
    match = LISTENING_ON_RE.search(line)
    if not match:
        return None
    addr = match.group("addr")
    had_host = False
    port: Optional[int] = None
    host: Optional[str] = None
    if addr.isdigit():
        port = int(addr)
    elif addr.startswith("["):
        ipv6_match = re.match(r"\[(.+)\]:(\d+)$", addr)
        if ipv6_match:
            host = ipv6_match.group(1)
            port = int(ipv6_match.group(2))
            had_host = True
    else:
        host_match = re.match(r"([^:]+):(\d+)$", addr)
        if host_match:
            host = host_match.group(1)
            port = int(host_match.group(2))
            had_host = True
    if port is None:
        return None
    return ListeningInfo(port=port, had_host=had_host, host=host, addr_span=match.span("addr"))


def is_loopback_host(host: str) -> bool:
    trimmed = (host or "").strip().lower()
    return trimmed in ("127.0.0.1", "localhost", "::1")


def rewrite_listening_on_line(line: str, info: ListeningInfo, proxy_port: int) -> str:
    replacement = f"127.0.0.1:{proxy_port}" if info.had_host else str(proxy_port)
    return f"{line[: info.addr_span[0]]}{replacement}{line[info.addr_span[1] :]}"


def rewrite_host_binding(line: str) -> Tuple[str, bool]:
    if "--host=127.0.0.1" not in line:
        return line, False
    rewritten = HOST_127_RE.sub(r"--host=\g<quote>0.0.0.0\g<quote>", line)
    return rewritten, rewritten != line


def ensure_on_host_in_command_shell(
    line: str, skip_injection: bool = False
) -> Tuple[str, bool]:
    if not COMMAND_SHELL_RE.search(line):
        return line, False
    if ON_HOST_RE.search(line):
        rewritten = ON_HOST_RE.sub(
            r"--on-host=\g<quote>0.0.0.0\g<quote>", line, count=1
        )
        return rewritten, True
    if skip_injection:
        return line, False

    def inject(match: re.Match[str]) -> str:
        return f"{match.group(0)} --on-host=0.0.0.0"

    rewritten = COMMAND_SHELL_RE.sub(inject, line, count=1)
    return rewritten, True


def ensure_on_host_in_listen_args(line: str) -> Tuple[str, bool]:
    match = LISTEN_ARGS_RE.search(line)
    if not match:
        return line, False
    body = match.group("body")
    if ON_HOST_RE.search(body):
        new_body = ON_HOST_RE.sub(r"--on-host=\g<quote>0.0.0.0\g<quote>", body, count=1)
    else:
        spacer = " " if body and not body.endswith(" ") else ""
        new_body = f"{body}{spacer}--on-host=0.0.0.0"
    rewritten = line[: match.start("body")] + new_body + line[match.end("body") :]
    return rewritten, True


def apply_stdin_rewrite(
    line: str,
    state: StdinRewriteState,
    logger: logging.Logger,
) -> str:
    rewritten = line
    if "--host=127.0.0.1" in rewritten:
        if LEGACY_HOST_REWRITE_RE.search(rewritten):
            rewritten = rewritten.replace("127.0.0.1", "0.0.0.0")
        elif not state.warned_unmatched_host:
            logger.warning("unanticipated localhost line found: %s", line.rstrip())
            state.warned_unmatched_host = True
    elif not state.listen_args_rewritten and '"$CLI_PATH" command-shell' in rewritten:
        if LEGACY_CLI_CMD_RE.search(rewritten):
            rewritten = re.sub(
                LEGACY_CLI_CMD_RE,
                r"\g<1> --on-host=0.0.0.0 \g<2>",
                rewritten,
            )
        elif not state.warned_unmatched_host:
            logger.warning("unanticipated localhost line found: %s", line.rstrip())
            state.warned_unmatched_host = True
    elif "LISTEN_ARGS=" in rewritten:
        if LEGACY_LISTEN_ARGS_RE.search(rewritten):
            rewritten = re.sub(
                LEGACY_LISTEN_ARGS_RE,
                r"\g<1> --on-host=0.0.0.0 ",
                rewritten,
            )
            state.listen_args_rewritten = True
        elif not state.warned_listen_args_unparsed:
            logger.warning("unanticipated localhost line found: %s", line.rstrip())
            state.warned_listen_args_unparsed = True
    return rewritten


def expand_pid_token(path: str, pid: int) -> str:
    return path.replace("[PID]", str(pid))


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


def wait_for_job_running(job_id: str, timeout: int, logger: logging.Logger) -> bool:
    deadline = time.time() + max(0, timeout)
    last_state: Optional[str] = None
    while True:
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

        # No active job found: create a new one in the user-specific directory.
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


def compute_log_level(verbose: int, quiet: int) -> int:
    level = logging.ERROR - (10 * verbose) + (10 * quiet)
    if level < logging.DEBUG:
        level = logging.DEBUG
    if level > logging.CRITICAL:
        level = logging.CRITICAL
    return level


def build_salloc_command(
    invoker: Optional[WorkgroupInvoker],
    workgroup: Optional[str],
    salloc_args: list[str],
) -> list[str]:
    return build_slurm_command(invoker, workgroup, ["salloc", *salloc_args])


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


class ProxyServer(threading.Thread):
    def __init__(
        self,
        state: SharedState,
        condition: threading.Condition,
        listen_host: str,
        listen_port: int,
        backlog: int,
        byte_limit: int,
        logger: logging.Logger,
    ) -> None:
        super().__init__(daemon=True)
        self._state = state
        self._condition = condition
        self._listen_host = listen_host
        self._listen_port = listen_port
        self._backlog = backlog
        self._byte_limit = byte_limit
        self._logger = logger
        self._loop_ready = threading.Event()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._start_event: Optional[asyncio.Event] = None
        self._stop_event: Optional[asyncio.Event] = None

    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._start_event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._loop_ready.set()
        try:
            loop.run_until_complete(self._main())
        finally:
            loop.close()

    def start_listening(self) -> None:
        self._loop_ready.wait()
        if self._loop and self._start_event:
            self._loop.call_soon_threadsafe(self._start_event.set)

    def stop(self) -> None:
        if not self._loop_ready.wait(timeout=1):
            return
        if self._loop and self._stop_event:
            self._loop.call_soon_threadsafe(self._stop_event.set)

    async def _main(self) -> None:
        assert self._start_event is not None
        assert self._stop_event is not None

        wait_start = asyncio.create_task(self._start_event.wait())
        wait_stop = asyncio.create_task(self._stop_event.wait())
        done, pending = await asyncio.wait(
            {wait_start, wait_stop},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        if self._stop_event.is_set():
            return

        try:
            server = await asyncio.start_server(
                self._handle_client,
                host=self._listen_host,
                port=self._listen_port,
                backlog=self._backlog,
                reuse_port=True,
            )
        except Exception:
            self._logger.error("Failed to start proxy server.", exc_info=True)
            with self._condition:
                self._state.proxy_failed = True
                self._condition.notify_all()
            return

        sockets = server.sockets or []
        if not sockets:
            self._logger.error("Proxy server started without sockets; aborting.")
            with self._condition:
                self._state.proxy_failed = True
                self._condition.notify_all()
            server.close()
            await server.wait_closed()
            return

        proxy_port = sockets[0].getsockname()[1]
        with self._condition:
            self._state.proxy_port = proxy_port
            self._state.proxy_ready = True
            self._condition.notify_all()
        self._logger.info(
            "Proxy server listening on %s:%s", self._listen_host, proxy_port
        )

        await self._stop_event.wait()
        server.close()
        await server.wait_closed()

    async def _handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        with self._condition:
            target_host = self._state.target_host
            target_port = self._state.target_port
        if not target_host or not target_port:
            self._logger.error(
                "Proxy connection received before target host/port ready."
            )
            client_writer.close()
            await client_writer.wait_closed()
            return

        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                target_host, target_port
            )
        except Exception:
            self._logger.error(
                "Failed to connect to %s:%s", target_host, target_port, exc_info=True
            )
            client_writer.close()
            await client_writer.wait_closed()
            return

        async def pipe(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                while True:
                    data = await reader.read(self._byte_limit)
                    if not data:
                        break
                    writer.write(data)
                    await writer.drain()
            except Exception:
                self._logger.debug("Proxy pipe error.", exc_info=True)

        task_a = asyncio.create_task(pipe(client_reader, remote_writer))
        task_b = asyncio.create_task(pipe(remote_reader, client_writer))
        done, pending = await asyncio.wait(
            {task_a, task_b}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        for writer in (client_writer, remote_writer):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Slurm Connect Remote-SSH stdio+TCP proxy. "
            "Requires Python 3.9+. Do not print to stdout."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase verbosity."
    )
    parser.add_argument(
        "-q", "--quiet", action="count", default=0, help="Decrease verbosity."
    )
    parser.add_argument(
        "-l",
        "--log-file",
        metavar="PATH",
        help="Write logs to file (use [PID] token for process id). Logging is disabled unless set.",
    )
    parser.add_argument(
        "-0",
        "--tee-stdin",
        metavar="PATH",
        help="Write stdin copy to file (use [PID] token).",
    )
    parser.add_argument(
        "-1",
        "--tee-stdout",
        metavar="PATH",
        help="Write stdout copy to file (use [PID] token).",
    )
    parser.add_argument(
        "-2",
        "--tee-stderr",
        metavar="PATH",
        help="Write stderr copy to file (use [PID] token).",
    )
    parser.add_argument(
        "-b", "--backlog", type=int, default=8, help="TCP accept backlog."
    )
    parser.add_argument(
        "-B", "--byte-limit", type=int, default=4096, help="Max bytes per proxy read."
    )
    parser.add_argument(
        "-H",
        "--listen-host",
        default="127.0.0.1",
        help="Host interface for the local proxy.",
    )
    parser.add_argument(
        "-p", "--listen-port", type=int, default=0, help="Port for the local proxy."
    )
    parser.add_argument(
        "-g",
        "--group",
        "--workgroup",
        dest="workgroup",
        help="Workgroup name for workgroup -g.",
    )
    parser.add_argument(
        "-S",
        "--salloc-arg",
        action="append",
        default=[],
        help="Repeatable salloc argument (appended verbatim).",
    )
    parser.add_argument(
        "--session-mode",
        choices=["ephemeral", "persistent"],
        default="ephemeral",
        help="Session mode (persistent reuses an allocation).",
    )
    parser.add_argument(
        "--session-key",
        default="",
        help="Persistent session key (defaults to SSH alias when provided by the extension).",
    )
    parser.add_argument(
        "--session-client-id",
        default="",
        help="Persistent session client identifier (used for active markers).",
    )
    parser.add_argument(
        "--session-idle-timeout",
        type=int,
        default=0,
        help="Idle timeout seconds before cancel (0 disables).",
    )
    parser.add_argument(
        "--session-ready-timeout",
        type=int,
        default=300,
        help="Seconds to wait for persistent session to reach RUNNING.",
    )
    parser.add_argument(
        "--session-state-dir",
        default=DEFAULT_SESSION_STATE_DIR,
        help="Base directory for session state.",
    )
    return parser


def configure_logging(
    args: argparse.Namespace, stderr_stream: TeeStream
) -> logging.Logger:
    logger = logging.getLogger("slurm-connect.proxy")
    logger.setLevel(compute_log_level(args.verbose or 0, args.quiet or 0))
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    log_path = (args.log_file or DEFAULT_LOG_FILE_TEMPLATE or "").strip()
    if not log_path:
        logger.addHandler(logging.NullHandler())
        return logger
    log_path = os.path.expanduser(
        os.path.expandvars(expand_pid_token(log_path, os.getpid()))
    )
    log_dir = os.path.dirname(log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def open_optional_file(path: Optional[str], pid: int, label: str) -> Optional[object]:
    if not path:
        return None
    expanded = os.path.expanduser(expand_pid_token(path, pid))
    try:
        return open(expanded, "w", encoding="utf-8", newline="")
    except OSError as exc:
        raise RuntimeError(f"Failed to open {label} file {expanded}: {exc}") from exc


def terminate_process(proc: subprocess.Popen, logger: logging.Logger) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            logger.debug("Failed to kill process.", exc_info=True)


def resolve_remote_shell() -> str:
    shell = (os.environ.get("SHELL") or "/bin/bash").strip()
    if not shell:
        return "/bin/bash"
    return shell.split()[0]


def start_client_keepalive(
    session_dir: str,
    client_id: str,
    stale_seconds: int,
    shutdown_event: threading.Event,
    logger: logging.Logger,
) -> Optional[threading.Thread]:
    safe_id = sanitize_token(client_id or uuid.uuid4().hex)
    client_name = f"client-{safe_id}"
    clients_dir = os.path.join(session_dir, "clients")
    last_seen_path = os.path.join(session_dir, "last_seen")
    client_path = os.path.join(clients_dir, client_name)
    try:
        os.makedirs(clients_dir, exist_ok=True)
    except Exception:
        logger.error(
            "Failed to create clients directory %s", clients_dir, exc_info=True
        )
        return None

    cleanup_stale_markers(session_dir, stale_seconds, logger)

    def touch_marker() -> None:
        now = int(time.time())
        try:
            with open(client_path, "w", encoding="utf-8") as handle:
                handle.write(str(os.getpid()))
            os.utime(client_path, None)
        except Exception:
            logger.debug(
                "Failed to update client marker %s", client_path, exc_info=True
            )
        try:
            with open(last_seen_path, "w", encoding="utf-8") as handle:
                handle.write(str(now))
        except Exception:
            logger.debug("Failed to update last_seen %s", last_seen_path, exc_info=True)

    def cleanup_marker() -> None:
        now = int(time.time())
        try:
            if os.path.isfile(client_path):
                os.remove(client_path)
        except Exception:
            logger.debug(
                "Failed to remove client marker %s", client_path, exc_info=True
            )
        try:
            with open(last_seen_path, "w", encoding="utf-8") as handle:
                handle.write(str(now))
        except Exception:
            logger.debug("Failed to update last_seen %s", last_seen_path, exc_info=True)

    def worker() -> None:
        touch_marker()
        while not shutdown_event.is_set():
            if shutdown_event.wait(CLIENT_TOUCH_INTERVAL):
                break
            touch_marker()
        cleanup_marker()

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread


def main() -> int:
    if sys.version_info < (3, 9):
        sys.stderr.write("slurm-connect proxy requires Python 3.9+.\n")
        return 1

    parser = build_arg_parser()
    args = parser.parse_args()

    if args.byte_limit <= 0:
        sys.stderr.write("--byte-limit must be > 0.\n")
        return errno.EINVAL
    if args.backlog <= 0:
        sys.stderr.write("--backlog must be > 0.\n")
        return errno.EINVAL
    if args.listen_port < 0 or args.listen_port > 65535:
        sys.stderr.write("--listen-port must be between 0 and 65535.\n")
        return errno.EINVAL
    if args.session_idle_timeout < 0:
        sys.stderr.write("--session-idle-timeout must be >= 0.\n")
        return errno.EINVAL
    if args.session_ready_timeout < 0:
        sys.stderr.write("--session-ready-timeout must be >= 0.\n")
        return errno.EINVAL

    pid = os.getpid()
    try:
        tee_stdin = open_optional_file(args.tee_stdin, pid, "stdin tee")
        tee_stdout = open_optional_file(args.tee_stdout, pid, "stdout tee")
        tee_stderr = open_optional_file(args.tee_stderr, pid, "stderr tee")
    except RuntimeError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1

    def close_tees() -> None:
        for handle in (tee_stdin, tee_stdout, tee_stderr):
            try:
                if handle is not None:
                    handle.close()
            except Exception:
                pass

    stdout_stream = TeeStream(sys.stdout, tee_stdout)
    stderr_stream = TeeStream(sys.stderr, tee_stderr)
    try:
        logger = configure_logging(args, stderr_stream)
    except Exception as exc:
        sys.stderr.write(f"Failed to configure logging: {exc}\n")
        close_tees()
        return 1

    workgroup: Optional[str] = None
    try:
        invoker = resolve_workgroup_invoker(logger)
        if invoker:
            if invoker.mode == "shell":
                logger.info(
                    "workgroup not in PATH; using login shell for workgroup commands."
                )
            workgroup = args.workgroup or detect_workgroup(invoker, logger)
        else:
            if args.workgroup:
                logger.warning(
                    "workgroup command unavailable; ignoring --workgroup and running Slurm commands directly."
                )
            workgroup = None
    except Exception:
        close_tees()
        return errno.EINVAL

    shutdown_event = threading.Event()
    condition = threading.Condition()
    state = SharedState()
    marker = f"__SLURM_CONNECT_PROXY_{pid}_{uuid.uuid4().hex}__"
    client_thread: Optional[threading.Thread] = None

    session_mode = (args.session_mode or "ephemeral").strip().lower()
    exec_server_bootstrap = False
    if session_mode == "ephemeral":
        lock_path = resolve_ephemeral_lock_path(args.session_key or "")
        if lock_path:
            now = time.time()
            if is_recent_lock(lock_path, now):
                exec_server_bootstrap = True
                logger.info(
                    "Recent ephemeral lock detected; bypassing Slurm allocation for this connection."
                )
            else:
                touch_lock(lock_path, now)

    command: list[str]
    if session_mode == "persistent":
        session_key = (args.session_key or "").strip() or "default"
        state_dir = (args.session_state_dir or "").strip() or DEFAULT_SESSION_STATE_DIR
        try:
            session_info = ensure_persistent_session(
                invoker=invoker,
                workgroup=workgroup,
                salloc_args=args.salloc_arg,
                session_key=session_key,
                idle_timeout=args.session_idle_timeout,
                state_dir=state_dir,
                logger=logger,
            )
        except Exception:
            logger.critical("Failed to establish persistent session.", exc_info=True)
            close_tees()
            return 1
        if not wait_for_job_running(
            session_info.job_id, args.session_ready_timeout, logger
        ):
            close_tees()
            return 1
        chosen_node = choose_session_node(
            session_info.session_dir, session_info.job_id, logger
        )
        if not chosen_node:
            logger.warning(
                "Unable to resolve session node; launching srun without --nodelist."
            )
        else:
            with condition:
                state.target_host = chosen_node
                condition.notify_all()
        client_id = (
            args.session_client_id or ""
        ).strip() or f"{pid}-{uuid.uuid4().hex}"
        client_thread = start_client_keepalive(
            session_info.session_dir,
            client_id,
            session_info.stale_seconds,
            shutdown_event,
            logger,
        )
        if client_thread is None:
            logger.warning(
                "Failed to start session keepalive; idle tracking may be wrong."
            )
        # shell = resolve_remote_shell()
        # use_pty = sys.stdin.isatty() and sys.stdout.isatty()
        # command = build_srun_command(
        #     invoker,
        #     workgroup,
        #     session_info.job_id,
        #     shell,
        #     use_pty,
        #     chosen_node,
        # )
        shell_cmd = ["/bin/bash", "-l"]
        use_pty = sys.stdin.isatty() and sys.stdout.isatty()
        command = build_srun_command(
            invoker,
            workgroup,
            session_info.job_id,
            shell_cmd,
            use_pty,
            chosen_node,
        )
        logger.info("Using session %s (job %s).", session_key, session_info.job_id)
    else:
        if exec_server_bootstrap:
            command = ["/bin/bash", "-l"]
        else:
            command = build_salloc_command(invoker, workgroup, args.salloc_arg)

    logger.info("Launching remote shell: %s", " ".join(command))
    try:
        proc = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            encoding="utf-8",
            errors="replace",
        )
    except Exception:
        logger.critical("Failed to launch remote shell.", exc_info=True)
        shutdown_event.set()
        if client_thread:
            client_thread.join(timeout=1)
        close_tees()
        return 1

    if proc.stdin is None or proc.stdout is None or proc.stderr is None:
        logger.critical("Failed to open remote shell pipes.")
        shutdown_event.set()
        if client_thread:
            client_thread.join(timeout=1)
        close_tees()
        return 1

    proxy_server = ProxyServer(
        state=state,
        condition=condition,
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        backlog=args.backlog,
        byte_limit=args.byte_limit,
        logger=logger,
    )
    proxy_server.start()

    def request_shutdown(reason: str) -> None:
        if shutdown_event.is_set():
            return
        logger.info("Shutdown requested: %s", reason)
        shutdown_event.set()
        with condition:
            state.shutdown = True
            condition.notify_all()

    def maybe_start_proxy() -> None:
        with condition:
            if state.proxy_start_requested or state.proxy_ready or state.proxy_failed:
                return
            if state.target_host and state.target_port:
                state.proxy_start_requested = True
                proxy_server.start_listening()

    def stdout_worker() -> None:
        listening_info: Optional[ListeningInfo] = None
        listening_line: Optional[str] = None
        parsing_failed = False

        for line in iter(proc.stdout.readline, ""):
            if shutdown_event.is_set():
                break
            if marker in line:
                hostname = parse_hostname_marker(line, marker)
                if hostname:
                    with condition:
                        if not state.target_host:
                            state.target_host = hostname
                            condition.notify_all()
                    maybe_start_proxy()
                continue

            if "listeningOn=" in line:
                with condition:
                    if state.listening_handled:
                        continue
                    state.listening_seen = True
                    state.listening_handled = True
                info = parse_listening_on_line(line)
                if not info:
                    logger.error("Unable to parse listeningOn line: %s", line.rstrip())
                    parsing_failed = True
                    stdout_stream.write(line)
                    continue
                listening_info = info
                listening_line = line
                with condition:
                    if info.host and is_loopback_host(info.host):
                        state.target_host = info.host
                    state.target_port = info.port
                    condition.notify_all()
                maybe_start_proxy()

                rewritten = None
                while not shutdown_event.is_set():
                    with condition:
                        if state.proxy_ready and state.proxy_port is not None:
                            rewritten = rewrite_listening_on_line(
                                listening_line, listening_info, state.proxy_port
                            )
                            break
                        if state.proxy_failed:
                            break
                        if not state.proxy_start_requested and state.target_host:
                            state.proxy_start_requested = True
                            proxy_server.start_listening()
                        condition.wait(timeout=0.1)
                if rewritten:
                    stdout_stream.write(rewritten)
                else:
                    stdout_stream.write(listening_line)
                continue

            stdout_stream.write(line)

        with condition:
            listening_seen = state.listening_seen
            target_host = state.target_host
            target_port = state.target_port
        if (
            not parsing_failed
            and target_host
            and not target_port
            and not listening_seen
        ):
            logger.error("Target host discovered but no listeningOn= line detected.")
        request_shutdown("remote stdout closed")

    def stderr_worker() -> None:
        for line in iter(proc.stderr.readline, ""):
            if shutdown_event.is_set():
                break
            if marker in line:
                hostname = parse_hostname_marker(line, marker)
                if hostname:
                    with condition:
                        if not state.target_host:
                            state.target_host = hostname
                            condition.notify_all()
                    maybe_start_proxy()
                continue

            if "listeningOn=" in line:
                with condition:
                    if state.listening_handled:
                        stderr_stream.write(line)
                        continue
                    state.listening_seen = True
                    state.listening_handled = True
                info = parse_listening_on_line(line)
                if not info:
                    logger.error(
                        "Unable to parse listeningOn line (stderr): %s",
                        line.rstrip(),
                    )
                    stderr_stream.write(line)
                    continue
                listening_info = info
                listening_line = line
                with condition:
                    if info.host and is_loopback_host(info.host):
                        state.target_host = info.host
                    state.target_port = info.port
                    condition.notify_all()
                maybe_start_proxy()

                rewritten = None
                while not shutdown_event.is_set():
                    with condition:
                        if state.proxy_ready and state.proxy_port is not None:
                            rewritten = rewrite_listening_on_line(
                                listening_line, listening_info, state.proxy_port
                            )
                            break
                        if state.proxy_failed:
                            break
                        if not state.proxy_start_requested and state.target_host:
                            state.proxy_start_requested = True
                            proxy_server.start_listening()
                        condition.wait(timeout=0.1)
                if rewritten:
                    stdout_stream.write(rewritten)
                else:
                    stdout_stream.write(listening_line)
                stderr_stream.write(line)
                continue

            stderr_stream.write(line)

    def stdin_worker() -> None:
        marker_cmd = f'printf "%s\\n" "{marker}{MARKER_HOST_PREFIX}$(hostname)"\n'
        xdg_cmds = [
            "unset XDG_RUNTIME_DIR",
            'export XDG_RUNTIME_DIR="/tmp/$USER/vscode-xdg-${SLURM_JOB_ID:-$$}"',
            'mkdir -p "$XDG_RUNTIME_DIR" && chmod 700 "$XDG_RUNTIME_DIR"',
        ]
        try:
            proc.stdin.write(marker_cmd)
            proc.stdin.write("\n".join(xdg_cmds) + "\n")
            proc.stdin.flush()
        except Exception:
            logger.error("Failed to inject hostname marker.", exc_info=True)
            request_shutdown("failed to inject marker")
            return

        rewrite_state = StdinRewriteState()
        while not shutdown_event.is_set():
            line = sys.stdin.readline()
            if shutdown_event.is_set():
                break
            if line == "":
                request_shutdown("stdin closed")
                break
            if tee_stdin is not None:
                tee_stdin.write(line)
                tee_stdin.flush()
            outgoing = apply_stdin_rewrite(line, rewrite_state, logger)
            try:
                proc.stdin.write(outgoing)
                proc.stdin.flush()
            except Exception:
                logger.error("Failed to write to remote shell stdin.", exc_info=True)
                request_shutdown("stdin write failed")
                break
        try:
            proc.stdin.close()
        except Exception:
            pass

    stdout_thread = threading.Thread(target=stdout_worker, daemon=True)
    stderr_thread = threading.Thread(target=stderr_worker, daemon=True)
    stdin_thread = threading.Thread(target=stdin_worker, daemon=True)

    stdout_thread.start()
    stderr_thread.start()
    stdin_thread.start()

    try:
        while not shutdown_event.is_set():
            time.sleep(0.1)
    except KeyboardInterrupt:
        request_shutdown("keyboard interrupt")

    terminate_process(proc, logger)
    proxy_server.stop()

    stdout_thread.join(timeout=2)
    stderr_thread.join(timeout=2)
    stdin_thread.join(timeout=2)
    proxy_server.join(timeout=2)
    if client_thread:
        client_thread.join(timeout=2)

    close_tees()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
