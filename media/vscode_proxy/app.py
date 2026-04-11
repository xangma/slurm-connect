from __future__ import annotations

import errno
import logging
import os
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from typing import Optional

from .cli import build_arg_parser, configure_logging
from .models import (
    DEFAULT_SESSION_STATE_DIR,
    EPHEMERAL_LOCK_TTL_SECONDS,
    MARKER_HOST_PREFIX,
    STDERR_TAIL_MAX_CHARS,
    STDERR_TAIL_MAX_LINES,
    LocalProxyTunnelConfig,
    SessionInfo,
    SharedState,
    StdinRewriteState,
    TeeStream,
)
from .parsing import (
    apply_stdin_rewrite,
    is_loopback_host,
    parse_hostname_marker,
    parse_listening_on_line,
    resolve_connect_host,
    rewrite_listening_on_line,
)
from .slurm import (
    build_salloc_command,
    build_srun_command,
    detect_workgroup,
    ensure_persistent_session,
    is_recent_lock,
    maybe_auto_cancel_unready_persistent_job,
    resolve_ephemeral_lock_path,
    resolve_workgroup_invoker,
    touch_lock,
    wait_for_job_running,
    choose_session_node,
)
from .transport import (
    ProxyServer,
    build_login_proxy_shell_command,
    build_tunnel_shell_command,
    open_optional_file,
    start_client_keepalive,
    terminate_process,
)


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

    tunnel_config: Optional[LocalProxyTunnelConfig] = None
    if args.local_proxy_tunnel:
        missing_fields = []
        if not args.local_proxy_tunnel_host:
            missing_fields.append("local-proxy-tunnel-host")
        if args.local_proxy_tunnel_port <= 0 or args.local_proxy_tunnel_port > 65535:
            missing_fields.append("local-proxy-tunnel-port")
        if not args.local_proxy_auth_user:
            missing_fields.append("local-proxy-auth-user")
        if not args.local_proxy_auth_token:
            missing_fields.append("local-proxy-auth-token")
        if missing_fields:
            logger.warning(
                "Local proxy tunnel disabled; missing %s.",
                ", ".join(missing_fields),
            )
        else:
            tunnel_config = LocalProxyTunnelConfig(
                login_host=args.local_proxy_tunnel_host,
                login_port=args.local_proxy_tunnel_port,
                login_user=args.local_proxy_tunnel_user
                or None,
                proxy_user=args.local_proxy_auth_user,
                proxy_token=args.local_proxy_auth_token,
                no_proxy=args.local_proxy_no_proxy or "",
                timeout=args.local_proxy_tunnel_timeout
                if args.local_proxy_tunnel_timeout > 0
                else None,
            )

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
    persistent_ready_event = threading.Event()
    persistent_session_info: Optional[SessionInfo] = None
    client_thread: Optional[threading.Thread] = None
    proc_started_at = time.monotonic()
    shutdown_reason: Optional[str] = None
    shutdown_lock = threading.Lock()
    stderr_tail = deque(maxlen=STDERR_TAIL_MAX_LINES)
    stderr_tail_lock = threading.Lock()
    failure_diagnostics_logged = threading.Event()

    session_mode = (args.session_mode or "ephemeral").strip().lower()
    session_key_raw = (args.session_key or "").strip()
    logger.info(
        "Session bootstrap: mode=%s, session_key=%s, stdin_tty=%s, stdout_tty=%s, stderr_tty=%s",
        session_mode,
        session_key_raw or "(empty)",
        sys.stdin.isatty(),
        sys.stdout.isatty(),
        sys.stderr.isatty(),
    )
    exec_server_bootstrap = False
    if session_mode == "ephemeral" and args.ephemeral_exec_server_bypass:
        lock_path = resolve_ephemeral_lock_path(session_key_raw)
        if lock_path:
            logger.info(
                "Ephemeral lock candidate: %s (ttl=%ss)",
                lock_path,
                EPHEMERAL_LOCK_TTL_SECONDS,
            )
            now = time.time()
            if is_recent_lock(lock_path, now):
                exec_server_bootstrap = True
                logger.info(
                    "Recent ephemeral lock detected; bypassing Slurm allocation for this connection."
                )
            else:
                touch_lock(lock_path, now)
                logger.info("Ephemeral lock refreshed: %s", lock_path)
        else:
            logger.info("Ephemeral lock path unavailable; proceeding without bypass lock.")

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
        persistent_session_info = session_info
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
                "Failed to start session keepalive; auto-cancel protection may be less accurate."
            )
        if not wait_for_job_running(
            session_info.job_id,
            args.session_ready_timeout,
            logger,
            stop_event=shutdown_event,
        ):
            shutdown_event.set()
            if client_thread:
                client_thread.join(timeout=1)
            maybe_auto_cancel_unready_persistent_job(
                invoker,
                workgroup,
                persistent_session_info,
                ready_emitted=persistent_ready_event.is_set(),
                logger=logger,
                reason="session failed to reach RUNNING",
            )
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
            chosen_host = resolve_connect_host(chosen_node, logger)
            with condition:
                state.target_host = chosen_host or chosen_node
                condition.notify_all()
        shell_cmd = ["/bin/bash", "-l"]
        if tunnel_config:
            shell_cmd = build_tunnel_shell_command(shell_cmd, tunnel_config)
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
        shell_cmd = ["/bin/bash", "-l"]
        if tunnel_config:
            if exec_server_bootstrap:
                shell_cmd = build_login_proxy_shell_command(shell_cmd, tunnel_config)
            else:
                shell_cmd = build_tunnel_shell_command(shell_cmd, tunnel_config)
        use_pty = sys.stdin.isatty() and sys.stdout.isatty()
        if exec_server_bootstrap:
            command = shell_cmd
        else:
            command = build_salloc_command(
                invoker, workgroup, args.salloc_arg, shell_cmd, use_pty=use_pty
            )

    if session_mode == "persistent":
        logger.info("Connection path selected: persistent session via srun.")
    elif exec_server_bootstrap:
        logger.info("Connection path selected: login-shell bypass (no Slurm allocation).")
    else:
        logger.info("Connection path selected: ephemeral allocation via salloc+srun.")
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
        maybe_auto_cancel_unready_persistent_job(
            invoker,
            workgroup,
            persistent_session_info,
            ready_emitted=persistent_ready_event.is_set(),
            logger=logger,
            reason="remote shell launch failed",
        )
        close_tees()
        return 1

    if proc.stdin is None or proc.stdout is None or proc.stderr is None:
        logger.critical("Failed to open remote shell pipes.")
        shutdown_event.set()
        if client_thread:
            client_thread.join(timeout=1)
        maybe_auto_cancel_unready_persistent_job(
            invoker,
            workgroup,
            persistent_session_info,
            ready_emitted=persistent_ready_event.is_set(),
            logger=logger,
            reason="remote shell pipes unavailable",
        )
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

    def state_snapshot() -> dict:
        with condition:
            return {
                "target_host": state.target_host,
                "target_port": state.target_port,
                "listening_seen": state.listening_seen,
                "proxy_ready": state.proxy_ready,
                "proxy_failed": state.proxy_failed,
            }

    def log_state_snapshot(prefix: str, level: int = logging.INFO) -> None:
        snap = state_snapshot()
        logger.log(
            level,
            "%s target_host=%s target_port=%s listening_seen=%s proxy_ready=%s proxy_failed=%s",
            prefix,
            snap["target_host"] or "",
            snap["target_port"] if snap["target_port"] is not None else "",
            snap["listening_seen"],
            snap["proxy_ready"],
            snap["proxy_failed"],
        )

    def remember_stderr_line(raw: str) -> None:
        line = raw.rstrip("\r\n")
        if len(line) > STDERR_TAIL_MAX_CHARS:
            line = f"{line[:STDERR_TAIL_MAX_CHARS]}...[truncated]"
        with stderr_tail_lock:
            stderr_tail.append(line)

    def log_recent_stderr_tail(level: int, context: str) -> None:
        with stderr_tail_lock:
            lines = list(stderr_tail)
        if not lines:
            logger.log(level, "Recent stderr tail unavailable (%s).", context)
            return
        logger.log(level, "Recent stderr tail (%s, %d lines):", context, len(lines))
        for idx, line in enumerate(lines, start=1):
            logger.log(level, "stderr_tail[%02d]: %s", idx, line)

    def emit_failure_diagnostics(context: str) -> None:
        if failure_diagnostics_logged.is_set():
            return
        failure_diagnostics_logged.set()
        log_state_snapshot(f"Failure snapshot ({context}):", level=logging.ERROR)
        log_recent_stderr_tail(logging.ERROR, context)

    def request_shutdown(reason: str) -> None:
        if shutdown_event.is_set():
            return
        logger.info("Shutdown requested: %s", reason)
        with shutdown_lock:
            nonlocal shutdown_reason
            shutdown_reason = reason
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
        parsing_failed = False

        for line in iter(proc.stdout.readline, ""):
            if shutdown_event.is_set():
                break
            if marker in line:
                hostname = parse_hostname_marker(line, marker)
                if hostname:
                    resolved_host = resolve_connect_host(hostname, logger)
                    with condition:
                        if not state.target_host:
                            state.target_host = resolved_host or hostname
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
                    emit_failure_diagnostics("unparseable listeningOn in stdout")
                    parsing_failed = True
                    stdout_stream.write(line)
                    continue
                listening_line = line
                with condition:
                    if info.host and is_loopback_host(info.host) and not state.target_host:
                        state.target_host = info.host
                    state.target_port = info.port
                    condition.notify_all()
                maybe_start_proxy()

                rewritten = None
                proxy_failed = False
                while not shutdown_event.is_set():
                    with condition:
                        if state.proxy_ready and state.proxy_port is not None:
                            rewritten = rewrite_listening_on_line(
                                listening_line, info, state.proxy_port
                            )
                            break
                        if state.proxy_failed:
                            proxy_failed = True
                            break
                        if not state.proxy_start_requested and state.target_host:
                            state.proxy_start_requested = True
                            proxy_server.start_listening()
                        condition.wait(timeout=0.1)
                if rewritten:
                    persistent_ready_event.set()
                    stdout_stream.write(rewritten)
                else:
                    if proxy_failed:
                        logger.error(
                            "Proxy startup failed before listeningOn rewrite (stdout)."
                        )
                        emit_failure_diagnostics(
                            "proxy startup failed before rewrite (stdout)"
                        )
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
            emit_failure_diagnostics(
                "target host discovered but listeningOn line not observed"
            )
        request_shutdown("remote stdout closed")

    def stderr_worker() -> None:
        for line in iter(proc.stderr.readline, ""):
            if shutdown_event.is_set():
                break
            remember_stderr_line(line)
            if marker in line:
                hostname = parse_hostname_marker(line, marker)
                if hostname:
                    resolved_host = resolve_connect_host(hostname, logger)
                    with condition:
                        if not state.target_host:
                            state.target_host = resolved_host or hostname
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
                    emit_failure_diagnostics("unparseable listeningOn in stderr")
                    stderr_stream.write(line)
                    continue
                listening_line = line
                with condition:
                    if info.host and is_loopback_host(info.host) and not state.target_host:
                        state.target_host = info.host
                    state.target_port = info.port
                    condition.notify_all()
                maybe_start_proxy()

                rewritten = None
                proxy_failed = False
                while not shutdown_event.is_set():
                    with condition:
                        if state.proxy_ready and state.proxy_port is not None:
                            rewritten = rewrite_listening_on_line(
                                listening_line, info, state.proxy_port
                            )
                            break
                        if state.proxy_failed:
                            proxy_failed = True
                            break
                        if not state.proxy_start_requested and state.target_host:
                            state.proxy_start_requested = True
                            proxy_server.start_listening()
                        condition.wait(timeout=0.1)
                if rewritten:
                    persistent_ready_event.set()
                    stdout_stream.write(rewritten)
                else:
                    if proxy_failed:
                        logger.error(
                            "Proxy startup failed before listeningOn rewrite (stderr)."
                        )
                        emit_failure_diagnostics(
                            "proxy startup failed before rewrite (stderr)"
                        )
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
                logger.info("stdin closed; closing remote stdin without shutting down proxy.")
                try:
                    proc.stdin.close()
                except Exception:
                    pass
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

    proc_returncode, termination_mode = terminate_process(proc, logger)
    proxy_server.stop()

    stdout_thread.join(timeout=2)
    stderr_thread.join(timeout=2)
    stdin_thread.join(timeout=2)
    proxy_server.join(timeout=2)
    if client_thread:
        client_thread.join(timeout=2)

    with shutdown_lock:
        final_reason = shutdown_reason or "unknown"
    elapsed = time.monotonic() - proc_started_at
    if proc_returncode is None:
        logger.warning(
            "Remote shell exit status unavailable after %.2fs (termination=%s).",
            elapsed,
            termination_mode,
        )
    elif proc_returncode < 0:
        logger.info(
            "Remote shell exited after %.2fs via signal %d (termination=%s).",
            elapsed,
            -proc_returncode,
            termination_mode,
        )
    else:
        logger.info(
            "Remote shell exited after %.2fs with code %d (termination=%s).",
            elapsed,
            proc_returncode,
            termination_mode,
        )
    log_state_snapshot(f"Final state snapshot (reason={final_reason}):")

    if proc_returncode not in (None, 0):
        emit_failure_diagnostics(
            f"remote shell exited non-zero ({proc_returncode})"
        )
    maybe_auto_cancel_unready_persistent_job(
        invoker,
        workgroup,
        persistent_session_info,
        ready_emitted=persistent_ready_event.is_set(),
        logger=logger,
        reason=f"proxy shutdown ({final_reason})",
    )

    close_tees()

    return 0
