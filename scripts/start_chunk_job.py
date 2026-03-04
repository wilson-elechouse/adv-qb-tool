#!/usr/bin/env python3
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from chunk_job_runtime import DEFAULT_ROOT, launch_background, write_json


def default_workdir():
    DEFAULT_ROOT.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return DEFAULT_ROOT / f"run-{stamp}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--bill-rules", required=True)
    ap.add_argument("--dir")
    ap.add_argument("--chunk-size", type=int, default=10)
    ap.add_argument("--max-batches-per-run", type=int, default=1)
    ap.add_argument("--config")
    ap.add_argument("--ai-cmd")
    ap.add_argument("--ai-runtime-config", default="skills/adv-qbo-tool/references/config/ai-runtime.json")
    ap.add_argument("--history")
    ap.add_argument("--auto-threshold", type=float, default=0.85)
    ap.add_argument("--confirm-threshold", type=float, default=0.65)
    ap.add_argument("--rules-cache-dir")
    ap.add_argument("--rules-ttl-seconds", type=int, default=21600)
    ap.add_argument("--manual-rules-snapshot")
    ap.add_argument("--auto-continue-seconds", type=int, default=10)
    ap.add_argument("--confirmed", action="store_true")
    ap.add_argument("--wait", action="store_true", help="wait for the first batch to finish instead of starting in background")
    args = ap.parse_args()

    workdir = Path(args.dir).resolve() if args.dir else default_workdir().resolve()
    cmd = [
        sys.executable,
        str((Path(__file__).resolve().parent / "workflow.py").resolve()),
        "--file", str(Path(args.file).resolve()),
        "--bill-rules", str(Path(args.bill_rules).resolve()),
        "--dir", str(workdir),
        "--chunk-size", str(max(1, int(args.chunk_size))),
        "--max-batches-per-run", str(int(args.max_batches_per_run)),
        "--ai-runtime-config", str(args.ai_runtime_config),
        "--auto-threshold", str(args.auto_threshold),
        "--confirm-threshold", str(args.confirm_threshold),
        "--rules-ttl-seconds", str(int(args.rules_ttl_seconds)),
    ]
    if args.config:
        cmd += ["--config", str(Path(args.config).resolve())]
    if args.ai_cmd:
        cmd += ["--ai-cmd", args.ai_cmd]
    if args.history:
        cmd += ["--history", args.history]
    if args.rules_cache_dir:
        cmd += ["--rules-cache-dir", str(Path(args.rules_cache_dir).resolve())]
    if args.manual_rules_snapshot:
        cmd += ["--manual-rules-snapshot", str(Path(args.manual_rules_snapshot).resolve())]
    if args.confirmed:
        cmd.append("--confirmed")

    if args.wait:
        import subprocess
        p = subprocess.run(cmd, text=True, capture_output=True)
        if p.returncode != 0:
            raise RuntimeError((p.stderr or p.stdout or "start_chunk_job_failed").strip())
        print((p.stdout or "").strip())
        return

    plan_path = workdir / "driver_plan.start.json"
    write_json(plan_path, {"initial_command": cmd, "mode": "start"})
    driver_cmd = [
        sys.executable,
        str((Path(__file__).resolve().parent / "chunk_job_driver.py").resolve()),
        "--workdir", str(workdir),
        "--initial-command-file", str(plan_path.resolve()),
        "--auto-continue-seconds", str(max(0, int(args.auto_continue_seconds))),
    ]
    launch = launch_background(driver_cmd, workdir, mode="start")
    print(json.dumps({
        "ok": True,
        "mode": "start_chunk_job",
        "workdir": str(workdir),
        "state_file": str((workdir / "workflow_state.json").resolve()),
        "next_action": "check_status_later",
        "status_hint": "python skills/adv-qbo-tool/scripts/chunk_job_status.py",
        "resume_hint": "python skills/adv-qbo-tool/scripts/resume_chunk_job.py",
        "auto_continue_seconds": max(0, int(args.auto_continue_seconds)),
        **launch,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
