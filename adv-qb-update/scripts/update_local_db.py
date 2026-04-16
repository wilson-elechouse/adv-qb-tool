#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
from pathlib import Path


def skill_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_config_path() -> Path:
    return skill_root() / "references" / "config" / "collector-config.local.json"


def default_out_dir() -> Path:
    return skill_root().parent / "adv-qb-tool" / "tmp" / "collector_rules_cache"


def refresh_script_path() -> Path:
    return skill_root().parent / "adv-qb-tool" / "scripts" / "refresh_bill_rules.py"


def main():
    ap = argparse.ArgumentParser(description="Refresh the local ADV QB Collector rules cache")
    ap.add_argument("--config", default=str(default_config_path()), help="collector config json")
    ap.add_argument("--out-dir", default=str(default_out_dir()), help="rules cache directory")
    ap.add_argument("--tenant-id", help="optional tenant override")
    ap.add_argument("--dry-run", action="store_true", help="fetch and compare only, do not write snapshot")
    args = ap.parse_args()

    config_path = Path(args.config).resolve()
    out_dir = Path(args.out_dir).resolve()
    refresh_script = refresh_script_path().resolve()

    if not refresh_script.exists():
        raise RuntimeError(f"refresh_script_missing:{refresh_script}")
    if not config_path.exists():
        raise RuntimeError(f"config_file_missing:{config_path}")

    cmd = [
        sys.executable,
        str(refresh_script),
        "--config",
        str(config_path),
        "--out-dir",
        str(out_dir),
        "--set-latest",
        "true",
    ]
    if args.tenant_id:
        cmd += ["--tenant-id", str(args.tenant_id)]
    if args.dry_run:
        cmd += ["--dry-run"]

    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", check=False)
    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()
    if proc.returncode != 0:
        raise RuntimeError(stderr or stdout or f"refresh_failed:{proc.returncode}")

    result = json.loads(stdout) if stdout else {"ok": True}
    output = {
        "ok": bool(result.get("ok", True)),
        "mode": "adv_qb_update",
        "config": str(config_path),
        "out_dir": str(out_dir),
        "refresh_script": str(refresh_script),
        "refresh_result": result,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
