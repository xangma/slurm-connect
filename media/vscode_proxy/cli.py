from __future__ import annotations

import argparse
import logging
import os

from .models import DEFAULT_LOG_FILE_TEMPLATE, DEFAULT_SESSION_STATE_DIR, TeeStream
from .transport import expand_pid_token


def compute_log_level(verbose: int, quiet: int) -> int:
    level = logging.ERROR - (10 * verbose) + (10 * quiet)
    if level < logging.DEBUG:
        level = logging.DEBUG
    if level > logging.CRITICAL:
        level = logging.CRITICAL
    return level


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
        "--ephemeral-exec-server-bypass",
        action="store_true",
        help=(
            "In ephemeral mode, bypass Slurm allocation for a short-lived follow-up "
            "connection. Disabled by default."
        ),
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
    parser.add_argument(
        "--local-proxy-tunnel",
        action="store_true",
        help="Start a compute-node SSH tunnel for the local proxy.",
    )
    parser.add_argument(
        "--local-proxy-tunnel-host",
        default="",
        help="Login host for the compute-node proxy tunnel.",
    )
    parser.add_argument(
        "--local-proxy-tunnel-user",
        default="",
        help="SSH user for the compute-node proxy tunnel.",
    )
    parser.add_argument(
        "--local-proxy-tunnel-port",
        type=int,
        default=0,
        help="Login host port for the proxy tunnel.",
    )
    parser.add_argument(
        "--local-proxy-auth-user",
        default="",
        help="Proxy auth user for compute-node HTTP(S) requests.",
    )
    parser.add_argument(
        "--local-proxy-auth-token",
        default="",
        help="Proxy auth token for compute-node HTTP(S) requests.",
    )
    parser.add_argument(
        "--local-proxy-no-proxy",
        default="",
        help="NO_PROXY value for compute-node HTTP(S) requests.",
    )
    parser.add_argument(
        "--local-proxy-tunnel-timeout",
        type=int,
        default=0,
        help="ConnectTimeout (seconds) for compute-node SSH tunnel.",
    )
    parser.add_argument(
        "--local-proxy-probe-url",
        default="",
        help="Run a one-shot HTTP proxy probe against this URL after proxy env is set.",
    )
    parser.add_argument(
        "--local-proxy-probe-token",
        default="",
        help="Expected token for --local-proxy-probe-url responses.",
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
