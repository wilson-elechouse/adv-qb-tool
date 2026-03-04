#!/usr/bin/env python3
import argparse
from pathlib import Path

from chunk_job_runtime import (
    read_json,
    resolve_default_search_root,
    is_pid_running,
    tail_text,
)


def candidate_state_files(root: Path):
    for path in root.rglob("workflow_state.json"):
        try:
            if path.is_file():
                yield path
        except Exception:
            continue


def pick_latest_chunk_job(root: Path):
    candidates = []
    for state_path in candidate_state_files(root):
        try:
            state = read_json(state_path)
        except Exception:
            continue
        if state.get("mode") != "chunked_job":
            continue
        candidates.append((state_path.stat().st_mtime, state_path))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir")
    ap.add_argument("--root", default=None, help="search root when --workdir is omitted; defaults to workspace tmp/adv-qbo")
    args = ap.parse_args()

    if args.workdir:
        workdir = Path(args.workdir).resolve()
    else:
        primary_root, roots = resolve_default_search_root(args.root)
        state_path = None
        root = primary_root
        for root in roots:
            state_path = pick_latest_chunk_job(root)
            if state_path is not None:
                break
        if state_path is None:
            raise RuntimeError(f"no_chunk_job_found_under:{root}")
        workdir = state_path.parent
    summary_path = workdir / "chunk_job_summary.json"
    report_path = workdir / "chunk_job_report.json"
    state_path = workdir / "workflow_state.json"
    if not state_path.exists():
        raise RuntimeError(f"workflow_state_missing:{state_path}")

    summary = read_json(summary_path) if summary_path.exists() else {}
    report = read_json(report_path) if report_path.exists() else {}
    state = read_json(state_path)
    last_batch = summary.get("last_completed_batch") or {}
    control_path = workdir / "job_control.json"
    control = read_json(control_path) if control_path.exists() else {}
    stdout_log = Path(control.get("stdout_log")) if control.get("stdout_log") else None
    stderr_log = Path(control.get("stderr_log")) if control.get("stderr_log") else None
    running = is_pid_running(control.get("pid"))

    progress_text = summary.get("progress_text")
    if not progress_text:
        if running:
            progress_text = f"Job is still running in state {state.get('state')}."
        else:
            progress_text = f"Job state is {state.get('state')}."

    needs_user_action_count = int(report.get("needs_user_action_count") or 0)
    auto_continue_ready = (
        (summary.get("state") or state.get("state")) == "WAIT_NEXT_BATCH"
        and not running
        and int(report.get("failed_count") or 0) == 0
        and needs_user_action_count == 0
    )

    out = {
        "ok": True,
        "mode": "chunked_job_status",
        "workdir": str(workdir),
        "state": summary.get("state") or state.get("state"),
        "completed_batches": summary.get("completed_batches", 0),
        "total_batches": summary.get("total_batches", 0),
        "next_batch_index": summary.get("next_batch_index", 0),
        "next_action": summary.get("next_action"),
        "progress_text": progress_text,
        "success_count": report.get("success_count"),
        "failed_count": report.get("failed_count"),
        "pending_count": report.get("pending_count"),
        "needs_user_action_count": needs_user_action_count,
        "can_auto_continue": report.get("can_auto_continue"),
        "auto_continue_ready": auto_continue_ready,
        "failure_reason_summary": report.get("failure_reason_summary") or {},
        "report_file": str(report_path) if report_path.exists() else None,
        "process_running": running,
        "pid": control.get("pid"),
        "job_control_status": control.get("status"),
        "auto_continue_seconds": control.get("auto_continue_seconds"),
        "auto_continue_at": control.get("auto_continue_at"),
        "stdout_log": str(stdout_log) if stdout_log else None,
        "stderr_log": str(stderr_log) if stderr_log else None,
        "stdout_tail": tail_text(stdout_log) if stdout_log else "",
        "stderr_tail": tail_text(stderr_log) if stderr_log else "",
        "last_completed_batch_index": last_batch.get("batch_index"),
        "last_batch_recap": last_batch.get("recap"),
        "last_batch_record_indexes": last_batch.get("record_indexes") or [],
    }
    import json
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
