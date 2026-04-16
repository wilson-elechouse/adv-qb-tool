#!/usr/bin/env python3
import argparse
from collections import Counter
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


def pick_latest_workflow(root: Path):
    candidates = []
    for state_path in candidate_state_files(root):
        try:
            read_json(state_path)
        except Exception:
            continue
        candidates.append((state_path.stat().st_mtime, state_path))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def derive_needs_user_action_items(report: dict):
    explicit = report.get("needs_user_action_items")
    if explicit:
        return explicit
    derived = []
    for item in report.get("success_items", []) or []:
        unresolved = item.get("unresolved") or []
        status = str(item.get("status") or "")
        if unresolved or status == "needs_user_confirmation":
            derived.append(item)
    return derived


def load_match_meta(match_file: str | None):
    if not match_file:
        return {}
    try:
        obj = read_json(Path(match_file))
    except Exception:
        return {}
    recap = obj.get("recap") or obj.get("source_recap") or {}
    request_no = recap.get("request_no")
    bill_number = recap.get("bill_number")
    if not request_no:
        client_ref = (
            ((obj.get("canonical_bill") or {}).get("client_ref"))
            or (((obj.get("fields") or {}).get("client_ref") or {}).get("value"))
            or ""
        )
        if str(client_ref).startswith("PR-"):
            request_no = str(client_ref)[3:]
    return {
        "request_no": request_no,
        "bill_number": bill_number,
    }


def failure_codes(item: dict):
    codes = []
    for key in ("source_errors", "validation_issues", "unresolved"):
        for value in item.get(key) or []:
            text = str(value or "").strip()
            if text:
                codes.append(text)
    if codes:
        return list(dict.fromkeys(codes))
    status = str(item.get("status") or "").strip()
    return [status or "failed"]


def build_report_from_batch_summary(batch_summary: dict):
    success_items = []
    failed_items = []
    pending_items = []
    needs_user_action_items = []
    failure_reason_summary = Counter()
    for item in batch_summary.get("results", []) or []:
        status = str(item.get("status") or "").strip()
        match_file = item.get("match_file")
        meta = load_match_meta(match_file)
        report_item = {
            "record_index": item.get("record_index"),
            "request_no": meta.get("request_no"),
            "bill_number": meta.get("bill_number"),
            "status": status,
            "unresolved": item.get("unresolved") or [],
            "source_errors": item.get("source_errors") or [],
            "validation_issues": item.get("validation_issues") or [],
            "match_file": match_file,
        }
        if status == "ready":
            success_items.append(report_item)
            continue
        if status == "needs_user_confirmation":
            needs_user_action_items.append(report_item)
            continue
        if status in {"invalid_source_data", "failed", "error"}:
            failed_items.append(report_item)
            for code in failure_codes(report_item):
                failure_reason_summary[code] += 1
            continue
        if report_item["unresolved"]:
            needs_user_action_items.append(report_item)
            continue
        pending_items.append(report_item)

    return {
        "success_items": success_items,
        "failed_items": failed_items,
        "pending_items": pending_items,
        "needs_user_action_items": needs_user_action_items,
        "success_count": len(success_items),
        "failed_count": len(failed_items),
        "pending_count": len(pending_items),
        "can_auto_continue": False,
        "failure_reason_summary": dict(failure_reason_summary),
    }


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
            for root in roots:
                state_path = pick_latest_workflow(root)
                if state_path is not None:
                    break
        if state_path is None:
            raise RuntimeError(f"no_workflow_found_under:{root}")
        workdir = state_path.parent
    summary_path = workdir / "chunk_job_summary.json"
    report_path = workdir / "chunk_job_report.json"
    batch_summary_path = workdir / "batch" / "batch_match_summary.json"
    state_path = workdir / "workflow_state.json"
    if not state_path.exists():
        raise RuntimeError(f"workflow_state_missing:{state_path}")

    summary = read_json(summary_path) if summary_path.exists() else {}
    state = read_json(state_path)
    is_chunked = state.get("mode") == "chunked_job"
    report = read_json(report_path) if report_path.exists() else {}
    if not report and batch_summary_path.exists():
        report = build_report_from_batch_summary(read_json(batch_summary_path))
    last_batch = summary.get("last_completed_batch") or {}
    control_path = workdir / "job_control.json"
    control = read_json(control_path) if control_path.exists() else {}
    stdout_log = Path(control.get("stdout_log")) if control.get("stdout_log") else None
    stderr_log = Path(control.get("stderr_log")) if control.get("stderr_log") else None
    running = is_pid_running(control.get("pid"))
    state_value = summary.get("state") or state.get("state")
    failed_count = int(report.get("failed_count") or 0)

    progress_text = summary.get("progress_text")
    if not progress_text:
        if running:
            progress_text = f"Job is still running in state {state_value}."
        else:
            progress_text = f"Job state is {state_value}."

    needs_user_action_items = derive_needs_user_action_items(report)
    needs_user_action_count = len(needs_user_action_items)
    needs_user_action_preview = []
    for item in needs_user_action_items[:10]:
        needs_user_action_preview.append({
            "record_index": item.get("record_index"),
            "request_no": item.get("request_no"),
            "bill_number": item.get("bill_number"),
            "unresolved": item.get("unresolved") or [],
            "match_file": item.get("match_file"),
        })
    failed_preview = []
    for item in (report.get("failed_items") or [])[:10]:
        failed_preview.append({
            "record_index": item.get("record_index"),
            "request_no": item.get("request_no"),
            "bill_number": item.get("bill_number"),
            "reason": failure_codes(item),
            "match_file": item.get("match_file"),
        })
    auto_continue_ready = (
        state_value == "WAIT_NEXT_BATCH"
        and not running
        and failed_count == 0
        and needs_user_action_count == 0
    )
    output_ready = (
        state_value == "WAIT_CONFIRMATION"
        and failed_count == 0
        and needs_user_action_count == 0
    )
    final_submit_allowed = False
    if state_value == "WAIT_CONFIRMATION" and failed_count > 0:
        progress_text = (
            f"Preparation finished, but {failed_count} record(s) are blocked by source or validation errors. "
            "Review the failed items before finalizing output."
        )
    elif state_value == "WAIT_CONFIRMATION" and needs_user_action_count > 0:
        progress_text = (
            f"All batches are prepared, but {needs_user_action_count} record(s) still need user confirmation "
            "before final submit."
        )
    elif output_ready:
        progress_text = "All batches are prepared. No unresolved items remain. Output files are ready."

    next_action = summary.get("next_action")
    if state_value == "WAIT_CONFIRMATION" and failed_count > 0:
        next_action = "review_failed_items"
    elif state_value == "WAIT_CONFIRMATION" and needs_user_action_count > 0:
        next_action = "resolve_user_confirmation_items"
    elif output_ready:
        next_action = "output_ready"
    elif state_value == "WAIT_NEXT_BATCH" and not next_action and not running:
        next_action = "resume_next_batch"

    completed_batches = summary.get("completed_batches")
    total_batches = summary.get("total_batches")
    next_batch_index = summary.get("next_batch_index")
    last_completed_batch_index = last_batch.get("batch_index")
    last_batch_recap = last_batch.get("recap")
    last_batch_record_indexes = last_batch.get("record_indexes") or []
    if not is_chunked and batch_summary_path.exists():
        batch_results = (read_json(batch_summary_path).get("results") or [])
        completed_batches = 1
        total_batches = 1
        next_batch_index = 1
        last_completed_batch_index = 1
        last_batch_recap = (state.get("artifacts") or {}).get("confirmation_recap")
        last_batch_record_indexes = [item.get("record_index") for item in batch_results if item.get("record_index") is not None]

    out = {
        "ok": True,
        "mode": "chunked_job_status" if is_chunked else "workflow_status",
        "workdir": str(workdir),
        "state": state_value,
        "completed_batches": int(completed_batches or 0),
        "total_batches": int(total_batches or 0),
        "next_batch_index": int(next_batch_index or 0),
        "next_action": next_action,
        "progress_text": progress_text,
        "success_count": report.get("success_count"),
        "failed_count": failed_count,
        "pending_count": report.get("pending_count"),
        "needs_user_action_count": needs_user_action_count,
        "needs_user_action_preview": needs_user_action_preview,
        "failed_preview": failed_preview,
        "output_ready": output_ready,
        "final_submit_allowed": final_submit_allowed,
        "can_auto_continue": report.get("can_auto_continue"),
        "auto_continue_ready": auto_continue_ready,
        "failure_reason_summary": report.get("failure_reason_summary") or {},
        "report_file": str(report_path) if report_path.exists() else None,
        "batch_summary_file": str(batch_summary_path) if batch_summary_path.exists() else None,
        "published_result_file": ((state.get("artifacts") or {}).get("published_result_file") or None),
        "published_issues_file": ((state.get("artifacts") or {}).get("published_issues_file") or None),
        "published_readable_issues_file": ((state.get("artifacts") or {}).get("published_readable_issues_file") or None),
        "process_running": running,
        "pid": control.get("pid"),
        "job_control_status": control.get("status"),
        "auto_continue_seconds": control.get("auto_continue_seconds"),
        "auto_continue_at": control.get("auto_continue_at"),
        "stdout_log": str(stdout_log) if stdout_log else None,
        "stderr_log": str(stderr_log) if stderr_log else None,
        "stdout_tail": tail_text(stdout_log) if stdout_log else "",
        "stderr_tail": tail_text(stderr_log) if stderr_log else "",
        "last_completed_batch_index": last_completed_batch_index,
        "last_batch_recap": last_batch_recap,
        "last_batch_record_indexes": last_batch_record_indexes,
    }
    import json
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
