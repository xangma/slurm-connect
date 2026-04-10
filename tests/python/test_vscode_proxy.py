import importlib.util
import logging
import sys
from pathlib import Path


def load_proxy_module():
    module_path = Path(__file__).resolve().parents[2] / "media" / "vscode-proxy.py"
    spec = importlib.util.spec_from_file_location("vscode_proxy_under_test", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


proxy = load_proxy_module()
LOGGER = logging.getLogger("test.vscode_proxy")


def test_parse_listening_on_line_and_rewrite():
    modern = proxy.parse_listening_on_line("ready listeningOn==127.0.0.1:4141== token")
    assert modern is not None
    assert modern.port == 4141
    assert modern.had_host is True
    assert modern.host == "127.0.0.1"
    assert (
        proxy.rewrite_listening_on_line(
            "ready listeningOn==127.0.0.1:4141== token", modern, 5000
        )
        == "ready listeningOn==127.0.0.1:5000== token"
    )

    legacy = proxy.parse_listening_on_line("prefix listeningOn=5001 end")
    assert legacy is not None
    assert legacy.port == 5001
    assert legacy.had_host is False
    assert proxy.rewrite_listening_on_line("prefix listeningOn=5001 end", legacy, 6002) == "prefix listeningOn=6002 end"


def test_parse_hostname_marker_and_loopback_helpers():
    assert (
        proxy.parse_hostname_marker(
            "prefix READY HOSTNAME=node001.example tail", "READY"
        )
        == "node001.example"
    )
    assert proxy.parse_hostname_marker("prefix READY HOSTNAME=bad!host", "READY") is None
    assert proxy.is_loopback_host("localhost") is True
    assert proxy.is_loopback_host("node001") is False


def test_command_shell_and_listen_args_rewrites():
    rewritten, changed = proxy.ensure_on_host_in_command_shell(
        '"$CLI_PATH" command-shell --foo=bar'
    )
    assert changed is True
    assert '--on-host=0.0.0.0' in rewritten

    replaced, changed = proxy.ensure_on_host_in_command_shell(
        '"$CLI_PATH" command-shell --on-host=127.0.0.1 --foo=bar'
    )
    assert changed is True
    assert '--on-host=0.0.0.0' in replaced
    assert '127.0.0.1' not in replaced

    listen_args, changed = proxy.ensure_on_host_in_listen_args(
        'LISTEN_ARGS="--foo=bar --baz=qux"'
    )
    assert changed is True
    assert '--on-host=0.0.0.0' in listen_args


def test_apply_stdin_rewrite_handles_legacy_host_binding():
    state = proxy.StdinRewriteState()
    rewritten = proxy.apply_stdin_rewrite(
        '$args = @("--host=127.0.0.1", "--port=5000")\n',
        state,
        LOGGER,
    )
    assert "--host=0.0.0.0" in rewritten
    assert state.warned_unmatched_host is False


def test_sanitize_token_and_resolve_session_paths(tmp_path, monkeypatch):
    monkeypatch.setenv("USER", "alice/ops")

    assert proxy.sanitize_token("  team prod/blue  ") == "team-prod-blue"
    assert proxy.sanitize_token("...") == "default"

    paths = proxy.resolve_session_paths(str(tmp_path), "alpha/beta", LOGGER)
    assert paths.state_dir == str(tmp_path)
    assert paths.safe_user == "alice-ops"
    assert paths.safe_key == "alpha-beta"
    assert paths.user_dir.endswith("sessions/alice-ops/alpha-beta")


def test_lock_helpers_and_pid_expansion(tmp_path):
    lock_path = tmp_path / "ephemeral.lock"
    now = 100.0
    proxy.touch_lock(str(lock_path), now)

    assert proxy.expand_pid_token("/tmp/proxy-[PID].log", 4321) == "/tmp/proxy-4321.log"
    assert proxy.is_recent_lock(str(lock_path), now + 5) is True
    assert proxy.is_recent_lock(str(lock_path), now + 30) is False

    session_lock_path = proxy.resolve_ephemeral_lock_path("alpha/beta")
    assert session_lock_path is not None
    assert session_lock_path.endswith("alpha-beta.lock")


def test_build_workgroup_and_slurm_commands():
    direct = proxy.WorkgroupInvoker(mode="direct", command="/usr/local/bin/workgroup")
    shell = proxy.WorkgroupInvoker(mode="shell", command="workgroup")

    assert proxy.build_workgroup_command(direct, ["-g", "research"]) == [
        "/usr/local/bin/workgroup",
        "-g",
        "research",
    ]
    assert proxy.build_workgroup_command(shell, ["-g", "research"]) == [
        "bash",
        "-lc",
        "workgroup -g research",
    ]
    assert proxy.build_slurm_command(None, None, ["squeue", "-h"]) == ["squeue", "-h"]
    assert proxy.build_slurm_command(direct, "team", ["squeue", "-h"]) == [
        "/usr/local/bin/workgroup",
        "-g",
        "team",
        "--command",
        "@",
        "--",
        "squeue",
        "-h",
    ]


def test_parse_scontrol_fields_and_job_state_normalization():
    fields = proxy.parse_scontrol_fields(
        "JobId=123 JobState=RUNNING+ Partition=gpu\n Nodes=2"
    )
    assert fields == {
        "JobId": "123",
        "JobState": "RUNNING+",
        "Partition": "gpu",
        "Nodes": "2",
    }
    assert proxy.normalize_job_state(" running+completing ") == "RUNNING"


def test_resolve_node_address_prefers_slurm_nodeaddr(monkeypatch):
    class Result:
        returncode = 0
        stdout = "NodeName=c1 NodeAddr=172.19.0.7 NodeHostName=c1"

    def fake_run(*args, **kwargs):
        return Result()

    monkeypatch.setattr(proxy.subprocess, "run", fake_run)

    assert proxy.resolve_node_address("c1", LOGGER) == "172.19.0.7"


def test_choose_session_node_prefers_saved_node_and_updates_job_json(
    tmp_path, monkeypatch
):
    session_dir = str(tmp_path)
    job_json = {"node": "c2"}

    monkeypatch.setattr(proxy, "load_job_json", lambda *args, **kwargs: dict(job_json))
    monkeypatch.setattr(proxy, "get_job_nodelist", lambda *args, **kwargs: "c[1-2]")
    monkeypatch.setattr(proxy, "expand_nodelist", lambda *args, **kwargs: ["c1", "c2"])

    writes = []

    def fake_write_job_json(path, payload, logger):
        writes.append((path, payload))

    monkeypatch.setattr(proxy, "write_job_json", fake_write_job_json)

    assert proxy.choose_session_node(session_dir, "12345", LOGGER) == "c2"
    assert writes == []


def test_resolve_connect_host_uses_slurm_nodeaddr_when_hostname_is_not_resolvable(
    monkeypatch,
):
    def fake_getaddrinfo(host, *args, **kwargs):
        if host == "c1":
            raise proxy.socket.gaierror(-2, "Name or service not known")
        return [(proxy.socket.AF_INET, proxy.socket.SOCK_STREAM, 6, "", ("172.19.0.7", 0))]

    monkeypatch.setattr(proxy.socket, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(proxy, "resolve_node_address", lambda host, logger: "172.19.0.7")

    assert proxy.resolve_connect_host("c1", LOGGER) == "172.19.0.7"
    assert proxy.resolve_connect_host("127.0.0.1", LOGGER) == "127.0.0.1"


def test_parse_workgroup_output_and_job_json_round_trip(tmp_path):
    assert proxy.parse_workgroup_output("1 research\n2 analytics") == "research"
    assert proxy.parse_workgroup_output("no useful output") is None

    payload = {"job_id": "12345", "node": "node001"}
    proxy.write_job_json(str(tmp_path), payload, LOGGER)
    assert proxy.load_job_json(str(tmp_path), LOGGER) == payload


def test_split_and_expand_nodelists():
    assert proxy.split_nodelist("node[001-002],gpu[7-6],plain") == [
        "node[001-002]",
        "gpu[7-6]",
        "plain",
    ]
    assert proxy.expand_nodelist_fallback("node[001-003,005],gpu7") == [
        "node001",
        "node002",
        "node003",
        "node005",
        "gpu7",
    ]


def test_compute_log_level_clamps_between_debug_and_critical():
    assert proxy.compute_log_level(verbose=3, quiet=0) == logging.DEBUG
    assert proxy.compute_log_level(verbose=0, quiet=5) == logging.CRITICAL


def test_build_salloc_and_srun_commands():
    salloc = proxy.build_salloc_command(
        None,
        None,
        ["--time=01:00:00"],
        command=["/bin/bash"],
        use_pty=True,
    )
    assert salloc == [
        "salloc",
        "--time=01:00:00",
        "srun",
        "--nodes=1",
        "--ntasks=1",
        "--pty",
        "/bin/bash",
    ]

    srun = proxy.build_srun_command(
        None,
        None,
        "12345",
        ["/bin/bash", "-l"],
        use_pty=True,
        node="node001",
    )
    assert srun == [
        "srun",
        "--jobid",
        "12345",
        "--overlap",
        "--nodes=1",
        "--ntasks=1",
        "--nodelist",
        "node001",
        "--pty",
        "/bin/bash",
        "-l",
    ]


def test_build_proxy_env_exports_quotes_dollar_expansions():
    exports = proxy.build_proxy_env_exports(
        "http://proxy-user:$TOKEN@127.0.0.1:8080",
        "localhost,127.0.0.1",
    )
    assert exports[0] == 'export HTTP_PROXY="http://proxy-user:$TOKEN@127.0.0.1:8080"'
    assert exports[-2:] == [
        "export NO_PROXY=localhost,127.0.0.1",
        "export no_proxy=localhost,127.0.0.1",
    ]


def test_build_tunnel_shell_command_contains_proxy_bootstrap():
    config = proxy.LocalProxyTunnelConfig(
        login_host="login.example.com",
        login_port=3128,
        login_user="alice",
        proxy_user="proxy-user",
        proxy_token="secret-token",
        no_proxy="localhost,127.0.0.1",
        timeout=15,
    )

    command = proxy.build_tunnel_shell_command(["/bin/bash", "-l"], config)
    assert command[0:2] == ["/bin/bash", "-lc"]
    script = command[2]
    assert "ssh -o BatchMode=yes" in script
    assert "alice@login.example.com" in script
    assert "ConnectTimeout=15" in script
    assert 'export HTTP_PROXY="http://proxy-user:secret-token@127.0.0.1:$LOCAL_PROXY_PORT"' in script
    assert "export NO_PROXY=localhost,127.0.0.1" in script
    assert "exec /bin/bash" in script


def test_build_login_proxy_shell_command_exports_proxy_variables():
    config = proxy.LocalProxyTunnelConfig(
        login_host="login.example.com",
        login_port=3128,
        login_user=None,
        proxy_user="proxy-user",
        proxy_token="secret-token",
        no_proxy="localhost",
        timeout=None,
    )

    command = proxy.build_login_proxy_shell_command(["/usr/bin/env", "bash"], config)
    assert command[0:2] == ["/bin/bash", "-lc"]
    script = command[2]
    assert "export HTTP_PROXY=http://proxy-user:secret-token@127.0.0.1:3128" in script
    assert "export NO_PROXY=localhost" in script
    assert "exec /usr/bin/env bash" in script


def test_build_arg_parser_defaults():
    parser = proxy.build_arg_parser()
    args = parser.parse_args([])
    assert args.verbose == 0
    assert args.quiet == 0
    assert args.session_mode == "ephemeral"
    assert args.listen_host == "127.0.0.1"
    assert args.listen_port == 0
