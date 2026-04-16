#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path

from canonical_support import repo_path
from chunk_job_runtime import (
    read_json,
    resolve_default_search_root,
    launch_background,
    write_json,
)


def candidate_state_files(root: Path):
    for path in root.rglob("workflow_state.json"):
        try:
            if path.is_file():
                yield path
        except Exception:
            continue


def pick_latest_waiting_job(root: Path):
    candidates = []
    for state_path in candidate_state_files(root):
        try:
            state = read_json(state_path)
        except Exception:
            continue
        if state.get("mode") != "chunked_job":
            continue
        if state.get("state") != "WAIT_NEXT_BATCH":
            continue
        candidates.append((state_path.stat().st_mtime, state_path, state))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x[0], reverse=True)
    _, state_path, state = candidates[0]
    return state_path, state


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", help="explicit chunk-job workdir")
    ap.add_argument("--root", default=None, help="search root when --workdir is omitted; defaults to workspace tmp/adv-qbo")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--auto-continue-seconds", type=int, default=10)
    ap.add_argument("--wait", action="store_true", help="wait for the resumed batch to finish instead of starting in background")
    args = ap.parse_args()

    if args.workdir:
        workdir = Path(args.workdir).resolve()
        state_path = workdir / "workflow_state.json"
        if not state_path.exists():
            raise RuntimeError(f"workflow_state_missing:{state_path}")
        state = read_json(state_path)
    else:
        primary_root, roots = resolve_default_search_root(args.root)
        state_path, state = None, None
        root = primary_root
        for root in roots:
            state_path, state = pick_latest_waiting_job(root)
            if state_path is not None:
                break
        if state_path is None:
            raise RuntimeError(f"no_waiting_chunk_job_found_under:{root}")
        workdir = state_path.parent

    if state.get("mode") != "chunked_job":
        raise RuntimeError(f"not_chunked_job:{workdir}")
    if state.get("state") != "WAIT_NEXT_BATCH":
        raise RuntimeError(f"job_not_waiting_for_resume:{state.get('state')}")

    inputs = state.get("inputs") or {}
    chunk_metrics = (state.get("metrics") or {}).get("chunk_job") or {}
    chunk_size = inputs.get("chunk_size") or chunk_metrics.get("chunk_size") or 10
    max_batches_per_run = inputs.get("max_batches_per_run") or 1
    cmd = [
        sys.executable,
        str((Path(__file__).resolve().parent / "workflow.py").resolve()),
        "--file", str(Path(inputs["file"]).resolve()),
        "--bill-rules", str(Path(inputs["bill_rules"]).resolve()),
        "--dir", str(workdir.resolve()),
        "--chunk-size", str(chunk_size),
        "--max-batches-per-run", str(max_batches_per_run),
        "--ai-runtime-config", str(inputs.get("ai_runtime_config") or repo_path("references", "config", "ai-runtime.json")),
        "--field-mapping", str(Path(inputs.get("field_mapping") or repo_path("references", "config", "field-mapping.xnofi.toml")).resolve()),
        "--resume",
    ]
    if inputs.get("config"):
        cmd += ["--config", str(Path(inputs["config"]).resolve())]
    if inputs.get("history"):
        cmd += ["--history", str(inputs["history"])]
    if inputs.get("manual_rules_snapshot"):
        cmd += ["--manual-rules-snapshot", str(Path(inputs["manual_rules_snapshot"]).resolve())]
    if inputs.get("rules_cache_dir"):
        cmd += ["--rules-cache-dir", str(Path(inputs["rules_cache_dir"]).resolve())]
    if inputs.get("ai_cmd"):
        cmd += ["--ai-cmd", str(inputs["ai_cmd"])]
    if bool((state.get("flags") or {}).get("require_ai")):
        cmd.append("--require-ai")

    if args.dry_run:
        import json
        print(json.dumps({
            "ok": True,
            "mode": "resume_chunk_job",
            "workdir": str(workdir.resolve()),
            "state_file": str(state_path.resolve()),
            "command": cmd,
        }, ensure_ascii=False, indent=2))
        return

    if args.wait:
        p = subprocess.run(cmd, text=True, capture_output=True)
        if p.returncode != 0:
            raise RuntimeError((p.stderr or p.stdout or "resume_failed").strip())
        print((p.stdout or "").strip())
        return

    plan_path = workdir / "driver_plan.resume.json"
    write_json(plan_path, {"initial_command": cmd, "mode": "resume"})
    driver_cmd = [
        sys.executable,
        str((Path(__file__).resolve().parent / "chunk_job_driver.py").resolve()),
        "--workdir", str(workdir),
        "--initial-command-file", str(plan_path.resolve()),
        "--auto-continue-seconds", str(max(0, int(args.auto_continue_seconds))),
    ]
    launch = launch_background(driver_cmd, workdir, mode="resume")
    import json
    print(json.dumps({
        "ok": True,
        "mode": "resume_chunk_job",
        "workdir": str(workdir.resolve()),
        "state_file": str(state_path.resolve()),
        "next_action": "check_status_later",
        "auto_continue_seconds": max(0, int(args.auto_continue_seconds)),
        **launch,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
