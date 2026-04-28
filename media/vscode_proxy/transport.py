from __future__ import annotations

import asyncio
import logging
import os
import shlex
import subprocess
import threading
import time
import uuid
from typing import Optional, Tuple

from .models import (
    CLIENT_TOUCH_INTERVAL,
    LocalProxyTunnelConfig,
    SharedState,
)
from .slurm import cleanup_stale_markers, sanitize_token


def expand_pid_token(path: str, pid: int) -> str:
    return path.replace("[PID]", str(pid))


def quote_env_value(value: str) -> str:
    if "$" in value:
        return f'"{value}"'
    return shlex.quote(value)


def build_proxy_env_exports(proxy_url: str, no_proxy: str) -> list[str]:
    exports = [
        f"export HTTP_PROXY={quote_env_value(proxy_url)}",
        f"export HTTPS_PROXY={quote_env_value(proxy_url)}",
        f"export http_proxy={quote_env_value(proxy_url)}",
        f"export https_proxy={quote_env_value(proxy_url)}",
        f"export ALL_PROXY={quote_env_value(proxy_url)}",
        f"export all_proxy={quote_env_value(proxy_url)}",
    ]
    if no_proxy:
        exports.append(f"export NO_PROXY={quote_env_value(no_proxy)}")
        exports.append(f"export no_proxy={quote_env_value(no_proxy)}")
    return exports


def build_local_proxy_probe_commands(config: LocalProxyTunnelConfig) -> list[str]:
    if not config.probe_url:
        return []

    return [
        f"export SLURM_CONNECT_LOCAL_PROXY_PROBE_URL={shlex.quote(config.probe_url)}",
        f"export SLURM_CONNECT_LOCAL_PROXY_PROBE_TOKEN={shlex.quote(config.probe_token)}",
        'if [ -z "${PYTHON:-}" ]; then',
        "  PYTHON=$(command -v python3 || command -v python || true)",
        "fi",
        'if [ -z "$PYTHON" ]; then',
        '  echo "Slurm Connect: python not available for local proxy probe." >&2',
        "  exit 1",
        "fi",
        '"$PYTHON" - <<\'PY\'',
        "import base64",
        "import http.client",
        "import os",
        "import sys",
        "from urllib.parse import unquote, urlparse",
        "",
        "url = os.environ['SLURM_CONNECT_LOCAL_PROXY_PROBE_URL']",
        "token = os.environ.get('SLURM_CONNECT_LOCAL_PROXY_PROBE_TOKEN', '')",
        "proxy_url = os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')",
        "if not proxy_url:",
        "    sys.stderr.write('Slurm Connect: local proxy probe missing HTTP_PROXY.\\n')",
        "    sys.exit(1)",
        "proxy = urlparse(proxy_url)",
        "if proxy.scheme != 'http' or not proxy.hostname:",
        "    sys.stderr.write('Slurm Connect: local proxy probe requires an HTTP proxy.\\n')",
        "    sys.exit(1)",
        "headers = {}",
        "target = urlparse(url)",
        "if target.netloc:",
        "    headers['Host'] = target.netloc",
        "if token:",
        "    headers['X-Slurm-Connect-Proxy-Probe'] = token",
        "if proxy.username or proxy.password:",
        "    user = unquote(proxy.username or '')",
        "    password = unquote(proxy.password or '')",
        "    auth = base64.b64encode(f'{user}:{password}'.encode()).decode()",
        "    headers['Proxy-Authorization'] = f'Basic {auth}'",
        "conn = None",
        "try:",
        "    conn = http.client.HTTPConnection(",
        "        proxy.hostname, proxy.port or 80, timeout=30",
        "    )",
        "    conn.request('GET', url, headers=headers)",
        "    response = conn.getresponse()",
        "    body = response.read().decode('utf-8', 'replace')",
        "except Exception as exc:",
        "    sys.stderr.write(f'Slurm Connect: local proxy probe failed: {exc}\\n')",
        "    sys.exit(1)",
        "finally:",
        "    if conn is not None:",
        "        try:",
        "            conn.close()",
        "        except Exception:",
        "            pass",
        "if response.status < 200 or response.status >= 300:",
        "    sys.stderr.write(",
        "        f'Slurm Connect: local proxy probe returned HTTP {response.status}.\\n'",
        "    )",
        "    sys.exit(1)",
        "if token and token not in body:",
        "    sys.stderr.write(",
        "        'Slurm Connect: local proxy probe response did not include token.\\n'",
        "    )",
        "    sys.exit(1)",
        "PY",
    ]


def build_tunnel_shell_command(
    shell_cmd: list[str], config: LocalProxyTunnelConfig
) -> list[str]:
    login_target = config.login_host
    if config.login_user:
        login_target = f"{config.login_user}@{login_target}"
    proxy_url = (
        f"http://{config.proxy_user}:{config.proxy_token}@127.0.0.1:$LOCAL_PROXY_PORT"
    )
    ssh_tokens = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
    ]
    if config.timeout:
        ssh_tokens.extend(["-o", f"ConnectTimeout={config.timeout}"])
    ssh_tokens.extend(
        [
            "-N",
            "-f",
            "-L",
            f"127.0.0.1:$LOCAL_PROXY_PORT:127.0.0.1:{config.login_port}",
            shlex.quote(login_target),
        ]
    )
    ssh_cmd = " ".join(ssh_tokens)
    pick_port = [
        "LOCAL_PROXY_PORT=\"\"",
        "PYTHON=$(command -v python3 || command -v python || true)",
        "if [ -n \"$PYTHON\" ]; then",
        "  LOCAL_PROXY_PORT=$($PYTHON - <<'PY'",
        "import socket",
        "s=socket.socket()",
        "s.bind(('127.0.0.1',0))",
        "print(s.getsockname()[1])",
        "s.close()",
        "PY",
        "  )",
        "fi",
        f"if [ -z \"$LOCAL_PROXY_PORT\" ]; then LOCAL_PROXY_PORT={config.login_port}; fi",
    ]
    start_tunnel = (
        f"{ssh_cmd} || {{ echo \"Slurm Connect: proxy tunnel SSH failed\" >&2; exit 1; }}"
    )
    wait_cmd = (
        "for i in $(seq 1 50); do "
        "(echo > /dev/tcp/127.0.0.1/$LOCAL_PROXY_PORT) >/dev/null 2>&1 && break; "
        "sleep 0.1; "
        "done"
    )
    verify_cmd = (
        "(echo > /dev/tcp/127.0.0.1/$LOCAL_PROXY_PORT) >/dev/null 2>&1 "
        "|| { echo \"Slurm Connect: proxy tunnel failed to open 127.0.0.1:$LOCAL_PROXY_PORT\" >&2; exit 1; }"
    )
    exports = build_proxy_env_exports(proxy_url, config.no_proxy)
    exec_shell_cmd = shell_cmd
    if shell_cmd and shell_cmd[0] == "/bin/bash" and "-l" in shell_cmd:
        exec_shell_cmd = ["/bin/bash"]
    exec_cmd = f"exec {shlex.join(exec_shell_cmd)}"
    probe = build_local_proxy_probe_commands(config)
    pieces = [*pick_port, start_tunnel, wait_cmd, verify_cmd, *exports, *probe, exec_cmd]
    wrapped = "\n".join(pieces)
    return ["/bin/bash", "-lc", wrapped]


def build_login_proxy_shell_command(
    shell_cmd: list[str], config: LocalProxyTunnelConfig
) -> list[str]:
    proxy_url = (
        f"http://{config.proxy_user}:{config.proxy_token}@127.0.0.1:{config.login_port}"
    )
    exports = build_proxy_env_exports(proxy_url, config.no_proxy)
    exec_shell_cmd = shell_cmd
    if shell_cmd and shell_cmd[0] == "/bin/bash" and "-l" in shell_cmd:
        exec_shell_cmd = ["/bin/bash"]
    exec_cmd = f"exec {shlex.join(exec_shell_cmd)}"
    probe = build_local_proxy_probe_commands(config)
    wrapped = "\n".join([*exports, *probe, exec_cmd])
    return ["/bin/bash", "-lc", wrapped]


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


def open_optional_file(path: Optional[str], pid: int, label: str) -> Optional[object]:
    if not path:
        return None
    expanded = os.path.expanduser(expand_pid_token(path, pid))
    try:
        return open(expanded, "w", encoding="utf-8", newline="")
    except OSError as exc:
        raise RuntimeError(f"Failed to open {label} file {expanded}: {exc}") from exc


def terminate_process(
    proc: subprocess.Popen, logger: logging.Logger
) -> Tuple[Optional[int], str]:
    if proc.poll() is not None:
        return proc.poll(), "already-exited"
    try:
        proc.terminate()
        proc.wait(timeout=5)
        return proc.poll(), "terminated"
    except Exception:
        try:
            proc.kill()
            proc.wait(timeout=5)
            return proc.poll(), "killed"
        except Exception:
            logger.debug("Failed to kill process.", exc_info=True)
            return proc.poll(), "kill-failed"


def start_client_keepalive(
    session_dir: str,
    client_id: str,
    stale_seconds: int,
    shutdown_event: threading.Event,
    logger: logging.Logger,
) -> Optional[threading.Thread]:
    safe_id = sanitize_token(client_id or uuid.uuid4().hex)
    instance_id = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
    client_name = f"client-{safe_id}.{instance_id}"
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
