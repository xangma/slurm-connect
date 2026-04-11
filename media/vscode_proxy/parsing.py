from __future__ import annotations

import logging
import re
import socket
import subprocess
from typing import Optional, Tuple

from .models import (
    COMMAND_SHELL_RE,
    HOSTNAME_VALUE_RE,
    HOST_127_RE,
    LEGACY_CLI_CMD_RE,
    LEGACY_HOST_REWRITE_RE,
    LEGACY_LISTENING_ON_RE,
    LEGACY_LISTENING_ON_SEARCH_RE,
    LEGACY_LISTEN_ARGS_RE,
    LISTENING_ON_RE,
    LISTEN_ARGS_RE,
    ListeningInfo,
    MARKER_HOST_PREFIX,
    ON_HOST_RE,
    StdinRewriteState,
)
from .slurm import parse_scontrol_fields


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


def resolve_node_address(node_name: str, logger: logging.Logger) -> Optional[str]:
    trimmed = (node_name or "").strip()
    if not trimmed:
        return None
    try:
        result = subprocess.run(
            ["scontrol", "show", "node", "-o", trimmed],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except Exception:
        logger.debug("Failed to query node metadata for %s.", trimmed, exc_info=True)
        return None
    if result.returncode != 0:
        return None
    fields = parse_scontrol_fields(result.stdout or "")
    for key in ("NodeAddr", "NodeHostName"):
        value = (fields.get(key) or "").strip()
        if value and value.lower() != "(null)":
            return value
    return None


def is_resolvable_host(host: str) -> bool:
    trimmed = (host or "").strip()
    if not trimmed:
        return False
    try:
        socket.getaddrinfo(trimmed, None)
        return True
    except socket.gaierror:
        return False


def resolve_connect_host(host: str, logger: logging.Logger) -> Optional[str]:
    trimmed = (host or "").strip()
    if not trimmed:
        return None
    if is_loopback_host(trimmed) or is_resolvable_host(trimmed):
        return trimmed
    resolved = resolve_node_address(trimmed, logger)
    if resolved and resolved != trimmed:
        logger.info("Resolved compute host %s -> %s via Slurm node metadata.", trimmed, resolved)
        return resolved
    return trimmed


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
