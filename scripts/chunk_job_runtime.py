#!/usr/bin/env python3
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROOT = WORKSPACE_ROOT / "tmp" / "adv-qbo"
FALLBACK_ROOT = WORKSPACE_ROOT / "tmp"


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def update_control(workdir: Path, **fields):
    control_path = workdir / "job_control.json"
    control = {}
    if control_path.exists():
        try:
            control = read_json(control_path)
        except Exception:
            control = {}
    control.update(fields)
    control["updated_at"] = datetime.now().isoformat(timespec="seconds")
    write_json(control_path, control)
    return control_path


def resolve_root(root_arg: str | None):
    if not root_arg:
        return DEFAULT_ROOT.resolve()
    p = Path(root_arg)
    if not p.is_absolute():
        p = WORKSPACE_ROOT / p
    return p.resolve()


def resolve_default_search_root(root_arg: str | None):
    primary = resolve_root(root_arg)
    if root_arg is not None:
        return primary, [primary]
    return primary, [primary, FALLBACK_ROOT.resolve()]


def is_pid_running(pid: int | None):
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def tail_text(path: Path, max_chars: int = 2000):
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def launch_background(cmd, workdir: Path, mode: str, exec_cwd: Path | None = None):
    workdir.mkdir(parents=True, exist_ok=True)
    logs_dir = workdir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    stdout_path = logs_dir / f"{mode}-{timestamp}.stdout.log"
    stderr_path = logs_dir / f"{mode}-{timestamp}.stderr.log"
    creationflags = 0
    creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    creationflags |= getattr(subprocess, "DETACHED_PROCESS", 0)
    with stdout_path.open("a", encoding="utf-8") as stdout_f, stderr_path.open("a", encoding="utf-8") as stderr_f:
        proc = subprocess.Popen(
            cmd,
            cwd=str((exec_cwd or WORKSPACE_ROOT).resolve()),
            stdout=stdout_f,
            stderr=stderr_f,
            text=True,
            creationflags=creationflags,
            start_new_session=(creationflags == 0),
        )

    control = {
        "mode": mode,
        "status": "running",
        "pid": proc.pid,
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "command": cmd,
        "exec_cwd": str((exec_cwd or WORKSPACE_ROOT).resolve()),
        "stdout_log": str(stdout_path.resolve()),
        "stderr_log": str(stderr_path.resolve()),
    }
    control_path = workdir / "job_control.json"
    write_json(control_path, control)
    return {
        "pid": proc.pid,
        "stdout_log": str(stdout_path.resolve()),
        "stderr_log": str(stderr_path.resolve()),
        "control_file": str(control_path.resolve()),
    }
