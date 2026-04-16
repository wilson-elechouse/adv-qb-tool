#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path

from canonical_support import repo_path


PROFILE_SPECS = {
    "codex": {
        "ai_runtime_config": repo_path("references", "config", "ai-runtime.codex.json"),
        "ai_cmd_envs": ["CODEX_AI_CMD", "ADV_QB_AI_CMD"],
        "strip_env_prefixes": ["OPENCLAW_AI_"],
        "strip_env_keys": ["OPENCLAW_AI_CMD"],
        "default_args": [
            "--codex-review-mode", "all-eligible",
            "--require-codex-review",
        ],
    },
    "openclaw": {
        "ai_runtime_config": repo_path("references", "config", "ai-runtime.openclaw.json"),
        "ai_cmd_envs": ["OPENCLAW_AI_CMD", "ADV_QB_AI_CMD"],
        "strip_env_prefixes": [],
        "strip_env_keys": [],
    },
}


def _resolve_ai_cmd(spec):
    for key in spec.get("ai_cmd_envs", []):
        value = str(os.environ.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _apply_env_profile(profile: str, spec):
    env = os.environ.copy()
    env["ADV_QB_RUNTIME_PROFILE"] = profile
    env["ADV_QB_AI_RUNTIME_CONFIG"] = str(Path(spec["ai_runtime_config"]).resolve())
    for key in list(env.keys()):
        if any(key.startswith(prefix) for prefix in spec.get("strip_env_prefixes", [])):
            env.pop(key, None)
    for key in spec.get("strip_env_keys", []):
        env.pop(key, None)
    return env


def _has_flag(argv, flag: str):
    return flag in list(argv or [])


def run_profile_entry(profile: str, script_name: str, argv):
    spec = PROFILE_SPECS[profile]
    script_path = (Path(__file__).resolve().parent / script_name).resolve()
    cmd = [sys.executable, str(script_path), *list(argv or [])]
    if not _has_flag(argv, "--ai-runtime-config"):
        cmd += ["--ai-runtime-config", str(Path(spec["ai_runtime_config"]).resolve())]
    if not _has_flag(argv, "--ai-cmd"):
        ai_cmd = _resolve_ai_cmd(spec)
        if ai_cmd:
            cmd += ["--ai-cmd", ai_cmd]
    default_args = list(spec.get("default_args") or [])
    idx = 0
    while idx < len(default_args):
        flag = default_args[idx]
        value = default_args[idx + 1] if idx + 1 < len(default_args) else None
        if not _has_flag(argv, flag):
            cmd.append(flag)
            if value is not None:
                cmd.append(value)
        idx += 2
    proc = subprocess.run(cmd, env=_apply_env_profile(profile, spec))
    return proc.returncode
