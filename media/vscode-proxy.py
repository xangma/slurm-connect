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

import importlib
import sys
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
THIS_DIR_STR = str(THIS_DIR)
if THIS_DIR_STR not in sys.path:
    sys.path.insert(0, THIS_DIR_STR)

PROXY_PACKAGE = importlib.import_module("vscode_proxy")
for exported_name in getattr(PROXY_PACKAGE, "__all__", []):
    globals()[exported_name] = getattr(PROXY_PACKAGE, exported_name)

main = PROXY_PACKAGE.main


if __name__ == "__main__":
    raise SystemExit(main())
