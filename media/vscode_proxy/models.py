from __future__ import annotations

import re
import threading
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
STDERR_TAIL_MAX_LINES = 20
STDERR_TAIL_MAX_CHARS = 500
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
PERSISTENT_UNREADY_CANCEL_GRACE_SECONDS = 5
PERSISTENT_UNREADY_CANCEL_POLL_SECONDS = 0.5


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
    submitted_new: bool = False


@dataclass(frozen=True)
class WorkgroupInvoker:
    mode: str
    command: str


@dataclass(frozen=True)
class LocalProxyTunnelConfig:
    login_host: str
    login_port: int
    login_user: Optional[str]
    proxy_user: str
    proxy_token: str
    no_proxy: str
    timeout: Optional[int]
    probe_url: str = ""
    probe_token: str = ""


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
