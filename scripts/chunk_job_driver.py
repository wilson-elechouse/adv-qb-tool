#!/usr/bin/env python3
import argparse
import subprocess
import sys
import time
from pathlib import Path

from chunk_job_runtime import read_json, update_control


def read_state(workdir: Path):
    state_path = workdir / "workflow_state.json"
    report_path = workdir / "chunk_job_report.json"
    summary_path = workdir / "chunk_job_summary.json"
    state = read_json(state_path) if state_path.exists() else {}
    report = read_json(report_path) if report_path.exists() else {}
    summary = read_json(summary_path) if summary_path.exists() else {}
    return state, report, summary


def compute_needs_user_action_count(report: dict):
    if report.get("needs_user_action_count") is not None:
        try:
            return int(report.get("needs_user_action_count") or 0)
        except Exception:
            return 0
    count = 0
    for item in report.get("success_items", []) or []:
        unresolved = item.get("unresolved") or []
        status = str(item.get("status") or "")
        if unresolved or status == "needs_user_confirmation":
            count += 1
    return count


def build_resume_command(workdir: Path):
    state_path = workdir / "workflow_state.json"
    if not state_path.exists():
        raise RuntimeError(f"workflow_state_missing:{state_path}")
    state = read_json(state_path)
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
        "--ai-runtime-config", str(inputs.get("ai_runtime_config") or "skills/adv-qbo-tool/references/config/ai-runtime.json"),
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
    if bool((state.get("flags") or {}).get("confirmed")):
        cmd.append("--confirmed")
    return cmd


def run_workflow_command(cmd, workdir: Path):
    proc = subprocess.run(
        cmd,
        cwd=str(Path(__file__).resolve().parents[3]),
        text=True,
        capture_output=True,
    )
    if proc.stdout:
        sys.stdout.write(proc.stdout)
        sys.stdout.flush()
    if proc.stderr:
        sys.stderr.write(proc.stderr)
        sys.stderr.flush()
    if proc.returncode != 0:
        update_control(
            workdir,
            status="error",
            last_returncode=proc.returncode,
        )
        raise RuntimeError((proc.stderr or proc.stdout or "chunk_job_driver_failed").strip())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--initial-command-file", required=True)
    ap.add_argument("--auto-continue-seconds", type=int, default=10)
    args = ap.parse_args()

    workdir = Path(args.workdir).resolve()
    plan_path = Path(args.initial_command_file).resolve()
    if not plan_path.exists():
        raise RuntimeError(f"initial_command_file_missing:{plan_path}")
    plan = read_json(plan_path)
    cmd = plan.get("initial_command") or []
    if not cmd:
        raise RuntimeError(f"initial_command_missing:{plan_path}")

    auto_continue_seconds = max(0, int(args.auto_continue_seconds))
    update_control(
        workdir,
        status="running",
        auto_continue_seconds=auto_continue_seconds,
        driver_plan=str(plan_path),
    )

    while True:
        run_workflow_command(cmd, workdir)
        state, report, summary = read_state(workdir)
        current_state = str(state.get("state") or "")
        failed_count = int(report.get("failed_count") or 0)
        needs_user_action_count = compute_needs_user_action_count(report)
        update_control(
            workdir,
            status=current_state.lower() if current_state else "unknown",
            last_known_state=current_state,
            failed_count=failed_count,
            needs_user_action_count=needs_user_action_count,
            progress_text=summary.get("progress_text"),
        )

        if current_state != "WAIT_NEXT_BATCH":
            break
        if failed_count > 0 or needs_user_action_count > 0 or auto_continue_seconds <= 0:
            break

        update_control(
            workdir,
            status="auto_continue_waiting",
            auto_continue_at=int(time.time()) + auto_continue_seconds,
        )
        time.sleep(auto_continue_seconds)
        state, report, summary = read_state(workdir)
        if str(state.get("state") or "") != "WAIT_NEXT_BATCH":
            break
        cmd = build_resume_command(workdir)

    state, report, summary = read_state(workdir)
    update_control(
        workdir,
        status=str(state.get("state") or "done").lower(),
        last_known_state=state.get("state"),
        finished_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        failed_count=int(report.get("failed_count") or 0),
        needs_user_action_count=compute_needs_user_action_count(report),
        progress_text=summary.get("progress_text"),
    )


if __name__ == "__main__":
    main()
