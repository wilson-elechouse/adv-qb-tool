#!/usr/bin/env python3
import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from canonical_support import repo_path, script_path

READY_RESULT_FILENAME = "canonical_bills.ready.json"
ISSUES_RESULT_FILENAME = "canonical_bills.issues.json"
READABLE_ISSUES_FILENAME = "canonical_bills.issues.readable.md"



def run(cmd, cwd=None):
    p = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "command failed").strip())
    return (p.stdout or "").strip()


def write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def parse_json_output(text: str):
    s = str(text or "").strip()
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        pass
    lines = [x for x in s.splitlines() if x.strip()]
    for ln in reversed(lines):
        try:
            return json.loads(ln)
        except Exception:
            continue
    return {}


def build_review_queue(step2_path: Path, summary_path: Path, out_path: Path, mode: str):
    run([
        "python", str(script_path("build_codex_review_queue.py")),
        "--step2", str(step2_path),
        "--summary", str(summary_path),
        "--out", str(out_path),
        "--mode", str(mode),
    ])
    return read_json(out_path)


def maybe_run_batch_category_ai_review(
    *,
    workdir: Path,
    step2_path: Path,
    summary_path: Path,
    bill_rules_path: Path,
    ai_cmd: str,
    ai_runtime_config: str,
    require_ai: bool,
    selection_mode: str,
):
    queue_path = workdir / "batch_category_ai_review_queue.json"
    queue_obj = build_review_queue(step2_path, summary_path, queue_path, selection_mode)
    initial_count = int(((queue_obj.get("counts") or {}).get("queued_records")) or 0)
    result = {
        "enabled": True,
        "selection_mode": selection_mode,
        "queue_file": str(queue_path.resolve()),
        "initial_queue_count": initial_count,
        "remaining_queue_count": initial_count,
        "ran": False,
        "status": "no_records" if initial_count <= 0 else "pending",
        "error": "",
    }
    if initial_count <= 0:
        return result
    if not str(ai_cmd or "").strip():
        result["status"] = "skipped_missing_ai_cmd"
        result["error"] = "batch_ai_cmd_missing"
        if require_ai:
            raise RuntimeError(result["error"])
        return result

    decisions_path = workdir / "batch_category_ai_review_decisions.json"
    payload_path = workdir / "batch_category_ai_review_payload.json"
    response_path = workdir / "batch_category_ai_review_response.json"
    audit_path = workdir / "batch_category_ai_review.json"

    started = time.perf_counter()
    try:
        run([
            "python", str(script_path("run_batch_category_ai_review.py")),
            "--queue", str(queue_path),
            "--ai-cmd", str(ai_cmd),
            "--out", str(decisions_path),
            "--payload-out", str(payload_path),
            "--response-out", str(response_path),
            "--audit-out", str(audit_path),
            "--runtime-config", str(ai_runtime_config or ""),
            "--require-complete", "true" if require_ai else "false",
        ])
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = str(exc)
        result["duration_ms"] = round((time.perf_counter() - started) * 1000.0, 2)
        result["payload_file"] = str(payload_path.resolve())
        result["response_file"] = str(response_path.resolve())
        result["audit_file"] = str(audit_path.resolve())
        if require_ai:
            raise
        return result

    run([
        "python", str(script_path("apply_codex_review_decisions.py")),
        "--step2", str(step2_path),
        "--decisions", str(decisions_path),
        "--bill-rules", str(bill_rules_path),
        "--judge-source", "ai_batch_review",
        "--provider", "ai_cmd_batch",
        "--review-mode", "ai_batch_review",
    ])
    run([
        "python", str(script_path("merge_step2_into_batch.py")),
        "--step2", str(step2_path),
        "--summary", str(summary_path),
    ])
    post_queue_obj = build_review_queue(step2_path, summary_path, queue_path, selection_mode)
    remaining = int(((post_queue_obj.get("counts") or {}).get("queued_records")) or 0)

    result.update(
        {
            "ran": True,
            "status": "completed" if remaining <= 0 else "partial",
            "remaining_queue_count": remaining,
            "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
            "payload_file": str(payload_path.resolve()),
            "response_file": str(response_path.resolve()),
            "decisions_file": str(decisions_path.resolve()),
            "audit_file": str(audit_path.resolve()),
            "counts": post_queue_obj.get("counts") or {},
        }
    )
    if require_ai and remaining > 0:
        raise RuntimeError(f"batch_ai_review_incomplete:{remaining}")
    return result


def _sha256_file(p: Path):
    return hashlib.sha256(p.read_text(encoding="utf-8").encode("utf-8")).hexdigest()


def fetch_live_bill_rules(config_path: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    raw = run([
        "python", str(script_path("refresh_bill_rules.py")),
        "--config", str(config_path.resolve()),
        "--out-dir", str(out_dir.resolve()),
        "--set-latest", "true",
    ])
    obj = json.loads(raw)
    if not obj.get("ok"):
        raise RuntimeError(f"refresh_bill_rules_failed:{raw}")
    return {
        "snapshot": Path(obj.get("snapshot")),
        "latest": Path(obj.get("latest")) if obj.get("latest") else (out_dir / "latest.json"),
        "manifest": Path(obj.get("manifest")) if obj.get("manifest") else (out_dir / "manifest.json"),
        "hash": obj.get("hash", ""),
        "taxonomy_check": obj.get("taxonomy_check") or {},
    }


def needs_live_refresh(summary_obj):
    for item in summary_obj.get("results", []):
        unresolved = set(item.get("unresolved") or [])
        if unresolved.intersection({"vendor_ref_text", "category_ref_text", "account_ref_text", "tax_code_ref_text", "tax_ref_text", "location_ref_text"}):
            return True
    return False


def resolve_effective_rules(input_rules: Path, cache_dir: Path, manual_snapshot: str = ""):
    if manual_snapshot:
        p = Path(manual_snapshot).resolve()
        if p.exists():
            return {"path": p, "source": "manual_refresh", "hash": _sha256_file(p)}
    latest = cache_dir / "latest.json"
    if latest.exists():
        return {"path": latest.resolve(), "source": "local_latest", "hash": _sha256_file(latest)}
    return {"path": input_rules.resolve(), "source": "local_input", "hash": _sha256_file(input_rules)}


def is_ttl_expired(path: Path, ttl_seconds: int):
    if not path.exists():
        return True
    age = datetime.now().timestamp() - path.stat().st_mtime
    return age > max(0, ttl_seconds)


def parse_uploaded_file(file_path: Path, out_path: Path, field_mapping_path: str = ""):
    cmd = [
        "python", str(script_path("parse_payment_request_xlsx.py")),
        "--file", str(file_path),
        "--out", str(out_path),
    ]
    if str(field_mapping_path or "").strip():
        cmd += ["--field-mapping", str(Path(field_mapping_path).resolve())]
    run(cmd)
    parsed = read_json(out_path)
    if not parsed.get("ok"):
        raise RuntimeError(f"parse_failed:{parsed.get('error','unknown')}")
    return parsed


def _safe_output_name(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return "run"
    return "".join(ch if (ch.isalnum() or ch in "._-") else "_" for ch in raw)


def normalize_ai_cmd(ai_cmd: str, ai_runtime_config: str = "") -> str:
    cmd = str(ai_cmd or "").strip()
    if not cmd:
        return ""
    if re.match(r'^(python|py)(?:\.exe)?\s+["\']?scripts[\\/]+ai_bridge\.py["\']?\s*$', cmd, flags=re.IGNORECASE):
        built = [sys.executable, str(script_path("ai_bridge.py").resolve())]
        if ai_runtime_config:
            built += ["--runtime-config", str(Path(ai_runtime_config).resolve())]
        return subprocess.list2cmdline(built)
    return cmd


def load_ai_runtime_required(ai_runtime_config: str) -> bool:
    cfg_path = Path(str(ai_runtime_config or "").strip())
    if not cfg_path.exists():
        return False
    try:
        obj = read_json(cfg_path)
    except Exception:
        return False
    return bool(obj.get("required"))


def derive_publish_run_name(workdir: Path) -> str:
    repo_root = repo_path()
    resolved = workdir.resolve()
    try:
        rel = resolved.relative_to(repo_root.resolve())
        parts = [p for p in rel.parts if p not in {".", ""}]
    except Exception:
        parts = [resolved.name]
    if parts and parts[0].lower() == "tmp":
        parts = parts[1:] or parts
    safe_parts = [_safe_output_name(p) for p in parts if _safe_output_name(p)]
    return "__".join(safe_parts) or _safe_output_name(resolved.name) or "run"


def plan_publish_targets(workdir: Path):
    run_name = derive_publish_run_name(workdir)
    root = repo_path("output")
    now = datetime.now()
    stamp = now.strftime("%Y%m%d_%H%M%S_%f")
    suffix = f"{stamp}.{run_name}"
    results_root = root / "results"
    ai_logs_root = root / "ai_logs"
    intermediate_root = ai_logs_root / "intermediate"
    return {
        "run_name": run_name,
        "timestamp": now.isoformat(timespec="seconds"),
        "results_root": results_root,
        "result_file": results_root / READY_RESULT_FILENAME,
        "issues_file": results_root / ISSUES_RESULT_FILENAME,
        "ai_logs_root": ai_logs_root,
        "issues_readable_file": ai_logs_root / f"issues_readable_report.{suffix}.md",
        "ai_log_file": ai_logs_root / f"account_ai_review_log.{suffix}.jsonl",
        "ai_summary_file": ai_logs_root / f"account_ai_review_summary.{suffix}.json",
        "ai_manifest_file": ai_logs_root / f"publish_manifest.{suffix}.json",
        "intermediate_root": intermediate_root,
        "step2_ai_judge_file": intermediate_root / f"step2_ai_judge.{suffix}.json",
        "codex_review_queue_file": intermediate_root / f"codex_review_queue.{suffix}.json",
        "vendor_ai_audit_file": intermediate_root / f"ai_audit.vendor.{suffix}.jsonl",
    }


def copy_to_path_if_exists(src, dest_path: Path):
    if not src:
        return None
    src_path = Path(src).resolve()
    if not src_path.exists():
        return None
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_path, dest_path)
    return dest_path


def load_ready_bills(source) -> list:
    if not source:
        return []
    try:
        obj = read_json(Path(source).resolve())
    except Exception:
        return []
    return copy.deepcopy(obj) if isinstance(obj, list) else []


def build_record_meta_for_index(record_lookup: dict, record_index):
    try:
        key = int(record_index)
    except Exception:
        key = record_index
    record = record_lookup.get(key) or {"record_index": record_index, "recap": {}}
    return build_record_brief(record)


def classify_prepare_result(status: str, unresolved: list, source_errors: list, validation_issues: list) -> str:
    normalized = str(status or "").strip()
    if normalized == "ready" and not unresolved and not source_errors and not validation_issues:
        return "success"
    if normalized in {"failed", "error"}:
        return "failed"
    if unresolved or source_errors or validation_issues or normalized in {"needs_user_confirmation", "invalid_source_data"}:
        return "needs_user_action"
    return "pending"


def build_issue_report_document(
    *,
    mode: str,
    state_value: str,
    total_records: int,
    ready_count: int,
    failed_items: list,
    needs_user_action_items: list,
    pending_items: list,
    source_report_file: str = "",
):
    failed_copy = copy.deepcopy(failed_items or [])
    needs_user_copy = copy.deepcopy(needs_user_action_items or [])
    pending_copy = copy.deepcopy(pending_items or [])
    issues = [
        {**copy.deepcopy(item), "issue_type": "failed"}
        for item in failed_copy
    ]
    issues.extend(
        {**copy.deepcopy(item), "issue_type": "needs_user_action"}
        for item in needs_user_copy
    )
    return {
        "ok": state_value != "ERROR",
        "mode": mode,
        "state": state_value,
        "total_records": int(total_records or 0),
        "ready_count": int(ready_count or 0),
        "failed_count": len(failed_copy),
        "needs_user_action_count": len(needs_user_copy),
        "pending_count": len(pending_copy),
        "issue_count": len(issues),
        "source_report_file": str(source_report_file or ""),
        "failed_items": failed_copy,
        "needs_user_action_items": needs_user_copy,
        "pending_items": pending_copy,
        "issues": issues,
    }


def build_issue_report_from_summary(workdir: Path, state: dict, summary_obj: dict, ready_count: int):
    record_lookup = {}
    parse_result = ((state.get("artifacts") or {}).get("parse_result")) if state else None
    if parse_result:
        try:
            parsed = read_json(Path(parse_result).resolve())
            record_lookup = build_record_lookup(parsed)
        except Exception:
            record_lookup = {}

    success_items = []
    failed_items = []
    pending_items = []
    needs_user_action_items = []
    for item in summary_obj.get("results") or []:
        status = str(item.get("status") or "")
        unresolved = copy.deepcopy(item.get("unresolved") or [])
        source_errors = copy.deepcopy(item.get("source_errors") or [])
        validation_issues = copy.deepcopy(item.get("validation_issues") or [])
        report_item = {
            **build_record_meta_for_index(record_lookup, item.get("record_index")),
            "stage": "prepare",
            "status": status,
            "match_file": item.get("match_file"),
            "unresolved": unresolved,
            "source_errors": source_errors,
            "validation_issues": validation_issues,
        }
        bucket = classify_prepare_result(status, unresolved, source_errors, validation_issues)
        if bucket == "success":
            success_items.append(report_item)
        elif bucket == "failed":
            report_item["reason"] = str(item.get("reason") or status or "failed")
            failed_items.append(report_item)
        elif bucket == "needs_user_action":
            needs_user_action_items.append(report_item)
        else:
            pending_items.append(report_item)

    total_records = len(record_lookup) or (
        len(success_items) + len(failed_items) + len(needs_user_action_items) + len(pending_items)
    )
    return build_issue_report_document(
        mode="workflow_issue_report",
        state_value=str(state.get("state") or "WAIT_CONFIRMATION"),
        total_records=total_records,
        ready_count=ready_count if ready_count else len(success_items),
        failed_items=failed_items,
        needs_user_action_items=needs_user_action_items,
        pending_items=pending_items,
        source_report_file=str((workdir / "batch" / "batch_match_summary.json").resolve()),
    )


def build_issue_report_from_chunk_report(report: dict, ready_count: int, source_report_file: str = ""):
    return build_issue_report_document(
        mode="chunked_job_issue_report",
        state_value=str(report.get("job_state") or ""),
        total_records=int(report.get("total_records") or 0),
        ready_count=ready_count if ready_count else int(report.get("success_count") or 0),
        failed_items=report.get("failed_items") or [],
        needs_user_action_items=report.get("needs_user_action_items") or [],
        pending_items=report.get("pending_items") or [],
        source_report_file=source_report_file,
    )


def collect_chunk_ready_bills(state: dict) -> list:
    merged = []
    for batch in state.get("batches") or []:
        if not batch:
            continue
        batch_workdir = Path(str(batch.get("workdir") or "")).resolve()
        candidates = [
            batch_workdir / READY_RESULT_FILENAME,
            batch_workdir / "batch" / READY_RESULT_FILENAME,
        ]
        for candidate in candidates:
            if not candidate.exists():
                continue
            try:
                data = read_json(candidate)
            except Exception:
                continue
            if isinstance(data, list):
                merged.extend(copy.deepcopy(data))
                break
    return merged


def materialize_publish_reports(
    workdir: Path,
    state_path: Path,
    summary_obj: dict | None,
    *,
    ready_bills=None,
    issues_report=None,
):
    state = read_json(state_path) if state_path.exists() else {}
    ready_payload = copy.deepcopy(ready_bills) if ready_bills is not None else load_ready_bills((summary_obj or {}).get("canonical_ready_file"))
    if issues_report is None:
        issues_payload = build_issue_report_from_summary(workdir, state, summary_obj or {}, len(ready_payload))
    else:
        issues_payload = copy.deepcopy(issues_report)
    local_ready_file = workdir / READY_RESULT_FILENAME
    local_issues_file = workdir / ISSUES_RESULT_FILENAME
    local_readable_issues_file = workdir / READABLE_ISSUES_FILENAME
    write_json(local_ready_file, ready_payload)
    write_json(local_issues_file, issues_payload)
    run([
        "python", str(script_path("render_issues_readable_report.py")),
        "--issues", str(local_issues_file),
        "--out", str(local_readable_issues_file),
    ])
    return {
        "local_ready_file": local_ready_file,
        "local_issues_file": local_issues_file,
        "local_readable_issues_file": local_readable_issues_file,
        "ready_bills": ready_payload,
        "issues_report": issues_payload,
    }


def publish_output_artifacts(
    workdir: Path,
    state_path: Path,
    recap_out: Path,
    summary_obj: dict | None,
    *,
    ready_bills=None,
    issues_report=None,
):
    publish = plan_publish_targets(workdir)
    local_reports = materialize_publish_reports(
        workdir,
        state_path,
        summary_obj,
        ready_bills=ready_bills,
        issues_report=issues_report,
    )

    result_file = copy_to_path_if_exists(local_reports["local_ready_file"], publish["result_file"])
    issues_file = copy_to_path_if_exists(local_reports["local_issues_file"], publish["issues_file"])
    issues_readable_file = copy_to_path_if_exists(local_reports["local_readable_issues_file"], publish["issues_readable_file"])
    ai_log_file = copy_to_path_if_exists((summary_obj or {}).get("account_ai_review_log_file"), publish["ai_log_file"])
    ai_summary_file = copy_to_path_if_exists((summary_obj or {}).get("account_ai_review_summary_file"), publish["ai_summary_file"])
    step2_ai_judge_file = copy_to_path_if_exists(workdir / "step2_ai_judge.json", publish["step2_ai_judge_file"])
    codex_review_queue_file = copy_to_path_if_exists(workdir / "codex_review_queue.json", publish["codex_review_queue_file"])
    vendor_ai_audit_file = copy_to_path_if_exists((summary_obj or {}).get("vendor_ai_audit_file"), publish["vendor_ai_audit_file"])

    write_json(
        publish["ai_manifest_file"],
        {
            "ok": True,
            "workdir": str(workdir.resolve()),
            "run_name": publish["run_name"],
            "published_at": publish["timestamp"],
            "result_file": str(result_file.resolve()) if result_file else "",
            "issues_file": str(issues_file.resolve()) if issues_file else "",
            "issues_readable_file": str(issues_readable_file.resolve()) if issues_readable_file else "",
            "ai_log_file": str(ai_log_file.resolve()) if ai_log_file else "",
            "ai_summary_file": str(ai_summary_file.resolve()) if ai_summary_file else "",
            "intermediate_files": {
                "step2_ai_judge_file": str(step2_ai_judge_file.resolve()) if step2_ai_judge_file else "",
                "codex_review_queue_file": str(codex_review_queue_file.resolve()) if codex_review_queue_file else "",
                "vendor_ai_audit_file": str(vendor_ai_audit_file.resolve()) if vendor_ai_audit_file else "",
            },
        },
    )
    return {
        "run_name": publish["run_name"],
        "result_file": str(publish["result_file"].resolve()),
        "issues_file": str(publish["issues_file"].resolve()),
        "local_result_file": str(local_reports["local_ready_file"].resolve()),
        "local_issues_file": str(local_reports["local_issues_file"].resolve()),
        "local_readable_issues_file": str(local_reports["local_readable_issues_file"].resolve()),
        "ai_logs_root": str(publish["ai_logs_root"].resolve()),
        "issues_readable_file": str(issues_readable_file.resolve()) if issues_readable_file else "",
        "ai_log_file": str(ai_log_file.resolve()) if ai_log_file else "",
        "ai_summary_file": str(ai_summary_file.resolve()) if ai_summary_file else "",
        "intermediate_ai_dir": str(publish["intermediate_root"].resolve()),
        "step2_ai_judge_file": str(step2_ai_judge_file.resolve()) if step2_ai_judge_file else "",
        "codex_review_queue_file": str(codex_review_queue_file.resolve()) if codex_review_queue_file else "",
        "vendor_ai_audit_file": str(vendor_ai_audit_file.resolve()) if vendor_ai_audit_file else "",
        "ai_logs_manifest": str(publish["ai_manifest_file"].resolve()),
    }


def chunk_records(records, chunk_size: int):
    size = max(1, int(chunk_size))
    return [records[i:i + size] for i in range(0, len(records), size)]


def build_chunk_parse(parsed_obj, chunk_records_list, chunk_index: int, total_chunks: int, source_parse_path: Path):
    chunk_obj = copy.deepcopy(parsed_obj)
    first_record = (chunk_records_list or [{}])[0] or {}
    chunk_obj["recap"] = copy.deepcopy(first_record.get("recap") or parsed_obj.get("recap") or {})
    chunk_obj["records"] = copy.deepcopy(chunk_records_list)
    chunk_obj["missing_required"] = copy.deepcopy(first_record.get("missing_required") or chunk_obj.get("missing_required") or [])
    rows = copy.deepcopy(chunk_obj.get("rows") or {})
    rows["approved_in_chunk"] = len(chunk_records_list)
    rows["chunk_size"] = len(chunk_records_list)
    rows["chunk_index"] = chunk_index + 1
    rows["total_chunks"] = total_chunks
    chunk_obj["rows"] = rows
    chunk_obj["chunk"] = {
        "index": chunk_index + 1,
        "total": total_chunks,
        "size": len(chunk_records_list),
        "record_index_start": (chunk_records_list[0].get("record_index") if chunk_records_list else None),
        "record_index_end": (chunk_records_list[-1].get("record_index") if chunk_records_list else None),
    }
    chunk_obj["source_parse_result"] = str(source_parse_path.resolve())
    return chunk_obj


def build_record_brief(record):
    recap = (record or {}).get("recap") or {}
    return {
        "record_index": record.get("record_index"),
        "vendor": recap.get("vendor", ""),
        "bill_number": recap.get("bill_number", ""),
        "request_no": recap.get("request_no", ""),
        "bill_date": recap.get("bill_date", ""),
        "due_date": recap.get("due_date", ""),
        "business_reason": recap.get("reason", ""),
    }


def build_record_lookup(parsed):
    lookup = {}
    for record in parsed.get("records") or []:
        try:
            lookup[int(record.get("record_index", 0))] = record
        except Exception:
            continue
    return lookup


def build_chunk_job_report(path: Path, state, parsed):
    record_lookup = build_record_lookup(parsed)
    success_items = []
    failed_items = []
    pending_items = []
    needs_user_action_items = []

    def record_meta(record_index):
        rec = record_lookup.get(int(record_index), {"record_index": record_index, "recap": {}})
        return build_record_brief(rec)

    for batch in state.get("batches", []):
        if not batch:
            continue
        batch_state = str(batch.get("state") or "")
        batch_workdir = Path(batch.get("workdir") or "")
        record_indexes = [int(x) for x in (batch.get("record_indexes") or []) if x is not None]

        if batch_state == "ERROR":
            for ridx in record_indexes:
                failed_items.append({
                    **record_meta(ridx),
                    "batch_index": batch.get("batch_index"),
                    "stage": "workflow",
                    "status": "failed",
                    "reason": batch.get("error") or "batch_error",
                })
            continue

        submit_path = batch_workdir / "batch" / "batch_submit_result.json"
        if submit_path.exists():
            try:
                submit_obj = read_json(submit_path)
                for item in submit_obj.get("results", []):
                    ridx = int(item.get("record_index", 0))
                    base = {
                        **record_meta(ridx),
                        "batch_index": batch.get("batch_index"),
                        "stage": "submit",
                    }
                    if item.get("ok"):
                        success_items.append({
                            **base,
                            "status": "submitted",
                            "submission_id": item.get("submission_id"),
                            "view_url": item.get("view_url"),
                        })
                    else:
                        failed_items.append({
                            **base,
                            "status": "failed",
                            "reason": item.get("error") or "submit_failed",
                            "action_required": item.get("action_required"),
                            "message": item.get("message"),
                            "existing_submission_id": item.get("existing_submission_id"),
                            "existing_view_url": item.get("existing_view_url"),
                        })
                continue
            except Exception:
                pass

        batch_summary_path = batch_workdir / "batch" / "batch_match_summary.json"
        if batch_summary_path.exists():
            try:
                batch_summary = read_json(batch_summary_path)
                for item in batch_summary.get("results", []):
                    ridx = int(item.get("record_index", 0))
                    report_item = {
                        **record_meta(ridx),
                        "batch_index": batch.get("batch_index"),
                        "stage": "prepare",
                        "status": item.get("status") or batch_state.lower(),
                        "match_file": item.get("match_file"),
                        "unresolved": item.get("unresolved") or [],
                    }
                    if report_item["unresolved"] or report_item["status"] == "needs_user_confirmation":
                        needs_user_action_items.append(report_item)
                    else:
                        success_items.append(report_item)
                continue
            except Exception:
                pass

        for ridx in record_indexes:
            pending_items.append({
                **record_meta(ridx),
                "batch_index": batch.get("batch_index"),
                "stage": "chunk_job",
                "status": "pending",
                "reason": "batch_not_processed_yet",
            })

    processed_record_indexes = {
        int(item["record_index"])
        for item in [*success_items, *failed_items, *pending_items, *needs_user_action_items]
        if item.get("record_index") is not None
    }
    for ridx, record in record_lookup.items():
        if ridx not in processed_record_indexes:
            pending_items.append({
                **build_record_brief(record),
                "batch_index": None,
                "stage": "chunk_job",
                "status": "pending",
                "reason": "not_processed_yet",
            })

    failure_reason_summary = {}
    for item in failed_items:
        reason = str(item.get("reason") or "unknown_failure")
        failure_reason_summary[reason] = failure_reason_summary.get(reason, 0) + 1

    can_auto_continue = (
        state.get("state") == "WAIT_NEXT_BATCH"
        and len(failed_items) == 0
        and len(needs_user_action_items) == 0
    )

    report = {
        "ok": state.get("state") != "ERROR",
        "mode": "chunked_job_report",
        "job_state": state.get("state"),
        "total_records": len(record_lookup),
        "success_count": len(success_items),
        "failed_count": len(failed_items),
        "pending_count": len(pending_items),
        "needs_user_action_count": len(needs_user_action_items),
        "can_auto_continue": can_auto_continue,
        "failure_reason_summary": failure_reason_summary,
        "success_items": success_items,
        "failed_items": failed_items,
        "pending_items": pending_items,
        "needs_user_action_items": needs_user_action_items,
        "retry_hint": f"python {script_path('retry_failed_chunk_job.py')}",
    }
    write_json(path, report)
    return report


def write_chunk_job_summary(path: Path, state):
    progress = state.get("progress") or {}
    batches = []
    last_batch = None
    for item in state.get("batches", []):
        if item:
            last_batch = item
        batches.append({
            "batch_index": item.get("batch_index"),
            "record_count": item.get("record_count"),
            "record_indexes": item.get("record_indexes"),
            "state": item.get("state"),
            "workdir": item.get("workdir"),
            "state_file": item.get("state_file"),
            "recap": item.get("recap"),
            "submit_result": item.get("submit_result"),
            "error": item.get("error"),
        })
    progress_text = (
        f"Processed {progress.get('completed_batches', 0)}/{progress.get('total_batches', 0)} batches"
        f" ({progress.get('total_records', 0)} records total)."
    )
    if state.get("state") == "WAIT_NEXT_BATCH":
        progress_text += " Resume to continue the next batch."
    elif state.get("state") == "WAIT_CONFIRMATION":
        progress_text += " All batches are prepared and waiting for confirmation."
    elif state.get("state") == "DONE":
        progress_text += " All batches are completed."
    summary = {
        "ok": state.get("state") != "ERROR",
        "mode": "chunked_job",
        "state": state.get("state"),
        "total_records": progress.get("total_records", 0),
        "total_batches": progress.get("total_batches", 0),
        "completed_batches": progress.get("completed_batches", 0),
        "next_batch_index": progress.get("next_batch_index", 0),
        "processed_batches_in_run": progress.get("processed_batches_in_run", 0),
        "last_completed_batch": {
            "batch_index": (last_batch or {}).get("batch_index"),
            "record_indexes": (last_batch or {}).get("record_indexes"),
            "recap": (last_batch or {}).get("recap"),
            "state": (last_batch or {}).get("state"),
        } if last_batch else None,
        "progress_text": progress_text,
        "batches": batches,
        "next_action": (
            "resume_next_batch"
            if state.get("state") == "WAIT_NEXT_BATCH"
            else ("wait_for_user_confirmation" if state.get("state") == "WAIT_CONFIRMATION" else "done")
        ),
    }
    write_json(path, summary)
    return summary


def maybe_run_chunk_job(args, workdir: Path, state_path: Path, state, parsed, parse_out: Path, ai_cmd: str, require_ai: bool):
    records = parsed.get("records") or []
    chunk_size = max(1, int(args.chunk_size))
    # A strict Codex review gate is easier to operate against a single merged queue.
    # Skip auto-splitting in that mode so one workbook produces one review pass.
    if args.require_codex_review or args.parsed or len(records) <= chunk_size:
        return False

    chunks = chunk_records(records, chunk_size)
    total_batches = len(chunks)
    chunk_root = workdir / "chunks"
    chunk_root.mkdir(parents=True, exist_ok=True)
    summary_path = workdir / "chunk_job_summary.json"
    report_path = workdir / "chunk_job_report.json"

    existing = {}
    if args.resume and state_path.exists():
        try:
            prev = read_json(state_path)
            if prev.get("mode") == "chunked_job":
                existing = prev
        except Exception:
            existing = {}

    if existing:
        state = existing
    else:
        state["mode"] = "chunked_job"
        state["artifacts"]["parse_result"] = str(parse_out.resolve())
        state["artifacts"]["chunk_root"] = str(chunk_root.resolve())
        state["artifacts"]["chunk_job_summary"] = str(summary_path.resolve())
        state["artifacts"]["chunk_job_report"] = str(report_path.resolve())
        state["batches"] = []

    state["inputs"]["file"] = str(Path(args.file).resolve())
    state["inputs"]["bill_rules"] = str(Path(args.bill_rules).resolve())
    state["inputs"]["config"] = str(Path(args.config).resolve()) if args.config else None
    state["inputs"]["require_ai"] = require_ai
    state["inputs"]["category_ai_mode"] = str(args.category_ai_mode)
    state["inputs"]["batch_ai_review_mode"] = str(args.batch_ai_review_mode)
    state["inputs"]["field_mapping"] = str(Path(args.field_mapping).resolve()) if args.field_mapping else ""
    state["flags"]["confirmed"] = bool(args.confirmed)
    state["flags"]["require_ai"] = require_ai
    state["flags"]["resume"] = bool(args.resume)

    progress = state.get("progress") or {}
    next_batch_index = int(progress.get("next_batch_index", 0)) if existing else 0
    completed_batches = int(progress.get("completed_batches", 0)) if existing else 0
    batches_meta = list(state.get("batches") or [])
    if len(batches_meta) < total_batches:
        batches_meta.extend({} for _ in range(total_batches - len(batches_meta)))
    state["batches"] = batches_meta
    state["progress"] = {
        "total_records": len(records),
        "total_batches": total_batches,
        "completed_batches": completed_batches,
        "next_batch_index": next_batch_index,
        "processed_batches_in_run": 0,
    }
    state["state"] = "CHUNK_JOB_RUNNING"
    write_json(state_path, state)

    remaining = max(0, total_batches - next_batch_index)
    max_batches = int(args.max_batches_per_run)
    batches_this_run = remaining if max_batches <= 0 else min(remaining, max(1, max_batches))
    end_batch_index = next_batch_index + batches_this_run

    for batch_idx in range(next_batch_index, end_batch_index):
        batch_records = chunks[batch_idx]
        batch_dir = chunk_root / f"batch-{batch_idx + 1:03d}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        batch_parse = batch_dir / "parse_result.chunk.json"
        write_json(batch_parse, build_chunk_parse(parsed, batch_records, batch_idx, total_batches, parse_out))

        batch_cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--parsed", str(batch_parse.resolve()),
            "--bill-rules", str(Path(args.bill_rules).resolve()),
            "--dir", str(batch_dir.resolve()),
            "--chunk-size", str(chunk_size),
            "--ai-cmd", ai_cmd,
            "--ai-runtime-config", args.ai_runtime_config,
            "--auto-threshold", str(args.auto_threshold),
            "--confirm-threshold", str(args.confirm_threshold),
            "--category-ai-mode", str(args.category_ai_mode),
            "--batch-ai-review-mode", str(args.batch_ai_review_mode),
            "--codex-review-mode", str(args.codex_review_mode),
            "--field-mapping", str(Path(args.field_mapping).resolve()),
        ]
        if args.config:
            batch_cmd += ["--config", str(Path(args.config).resolve())]
        if args.history:
            batch_cmd += ["--history", args.history]
        if args.rules_cache_dir:
            batch_cmd += ["--rules-cache-dir", args.rules_cache_dir]
        if args.manual_rules_snapshot:
            batch_cmd += ["--manual-rules-snapshot", args.manual_rules_snapshot]
        if args.confirmed:
            batch_cmd.append("--confirmed")
        if require_ai:
            batch_cmd.append("--require-ai")
        if args.require_codex_review:
            batch_cmd.append("--require-codex-review")

        batch_state_path = batch_dir / "workflow_state.json"
        batch_error = None
        batch_result = {}
        try:
            batch_raw = run(batch_cmd)
            batch_result = parse_json_output(batch_raw)
        except Exception as e:
            batch_error = str(e)
        batch_state = read_json(batch_state_path) if batch_state_path.exists() else {}
        batch_entry = {
            "batch_index": batch_idx + 1,
            "record_count": len(batch_records),
            "record_indexes": [r.get("record_index") for r in batch_records],
            "workdir": str(batch_dir.resolve()),
            "parsed": str(batch_parse.resolve()),
            "state_file": str(batch_state_path.resolve()),
            "state": batch_state.get("state") or ("ERROR" if batch_error else (batch_result.get("state") or "UNKNOWN")),
            "recap": batch_result.get("recap") or (batch_state.get("artifacts") or {}).get("confirmation_recap"),
            "submit_result": batch_result.get("submit_result") or (batch_state.get("artifacts") or {}).get("batch_submit_result"),
            "error": batch_state.get("error") or batch_error,
        }
        state["batches"][batch_idx] = batch_entry
        state["progress"]["completed_batches"] = batch_idx + 1
        state["progress"]["next_batch_index"] = batch_idx + 1
        state["progress"]["processed_batches_in_run"] = batch_idx + 1 - next_batch_index
        state["metrics"]["chunk_job"] = {
            "chunk_size": chunk_size,
            "processed_batches_in_run": state["progress"]["processed_batches_in_run"],
            "completed_batches": state["progress"]["completed_batches"],
            "total_batches": total_batches,
            "total_records": len(records),
        }
        write_json(state_path, state)
        write_chunk_job_summary(summary_path, state)
        build_chunk_job_report(report_path, state, parsed)

        if batch_entry["state"] == "ERROR":
            state["state"] = "ERROR"
            state["error"] = batch_entry["error"] or f"chunk_batch_failed:{batch_idx + 1}"
            write_json(state_path, state)
            write_chunk_job_summary(summary_path, state)
            build_chunk_job_report(report_path, state, parsed)
            raise RuntimeError(state["error"])

    completed_batches = int(state["progress"]["completed_batches"])
    state["state"] = "WAIT_CONFIRMATION"
    if completed_batches < total_batches:
        state["state"] = "WAIT_NEXT_BATCH"
    write_json(state_path, state)
    summary = write_chunk_job_summary(summary_path, state)
    report = build_chunk_job_report(report_path, state, parsed)
    last_batch = summary.get("last_completed_batch") or {}
    merged_ready_bills = collect_chunk_ready_bills(state)
    merged_issues_report = build_issue_report_from_chunk_report(report, len(merged_ready_bills), str(report_path.resolve()))
    published = publish_output_artifacts(
        workdir,
        state_path,
        workdir / "confirmation_recap.json",
        None,
        ready_bills=merged_ready_bills,
        issues_report=merged_issues_report,
    )
    state.setdefault("artifacts", {})["canonical_ready_file"] = published["local_result_file"]
    state["artifacts"]["issues_report_file"] = published["local_issues_file"]
    state["artifacts"]["published_result_file"] = published["result_file"]
    state["artifacts"]["published_issues_file"] = published["issues_file"]
    state["artifacts"]["published_ai_logs_manifest"] = published["ai_logs_manifest"]
    state["artifacts"]["published_run_name"] = published["run_name"]
    write_json(state_path, state)

    print(json.dumps({
        "ok": True,
        "mode": "chunked_job",
        "state": state["state"],
        "state_file": str(state_path.resolve()),
        "job_summary": str(summary_path.resolve()),
        "job_report": str(report_path.resolve()),
        "completed_batches": completed_batches,
        "total_batches": total_batches,
        "next_batch_index": state["progress"]["next_batch_index"],
        "processed_batches_in_run": state["progress"]["processed_batches_in_run"],
        "last_completed_batch_index": last_batch.get("batch_index"),
        "last_batch_recap": last_batch.get("recap"),
        "progress_text": summary.get("progress_text"),
        "success_count": report.get("success_count"),
        "failed_count": report.get("failed_count"),
        "pending_count": report.get("pending_count"),
        "published_result_file": published["result_file"],
        "published_issues_file": published["issues_file"],
        "next_action": summary.get("next_action"),
    }, ensure_ascii=False, indent=2))
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file")
    ap.add_argument("--parsed", help="optional parse_result.json input; skips S1 parsing")
    ap.add_argument("--bill-rules", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--chunk-size", type=int, default=10)
    ap.add_argument("--max-batches-per-run", type=int, default=1, help="when source rows exceed chunk-size, process at most N batches per invocation; 0 means all")
    ap.add_argument("--resume", action="store_true", help="resume a previously split chunk job in the same --dir")
    ap.add_argument("--confirmed", action="store_true", help="legacy flag; submit is disabled in output-only mode")
    ap.add_argument("--config", help="optional Collector config used only for bill-rules refresh")
    ap.add_argument("--ai-cmd", help="AI command for Step2 category judge (optional if configured in ai-runtime config or ADV_QB_AI_CMD / OPENCLAW_AI_CMD)")
    ap.add_argument("--ai-runtime-config", default=str(repo_path("references", "config", "ai-runtime.json")))
    ap.add_argument("--require-ai", action="store_true", help="fail when AI is unavailable or falls back instead of using a real AI result")
    ap.add_argument("--category-ai-mode", choices=["per-record", "batch-review"], default="per-record", help="per-record uses ai_cmd inside step2_match; batch-review lets code pick first and sends a single merged review payload to ai_cmd")
    ap.add_argument("--batch-ai-review-mode", choices=["review-worthy", "all-eligible"], default="all-eligible", help="record selection mode for the single batch AI review payload")
    ap.add_argument("--codex-review-mode", choices=["review-worthy", "all-eligible"], default="review-worthy", help="how many records to queue for Codex review when no external AI is wired")
    ap.add_argument("--require-codex-review", action="store_true", help="stop in WAIT_CODEX_REVIEW until Codex decisions are applied")
    ap.add_argument("--history", help="optional confirmed category history json")
    ap.add_argument("--auto-threshold", type=float, default=0.85)
    ap.add_argument("--confirm-threshold", type=float, default=0.65)
    ap.add_argument("--rules-cache-dir", help="optional directory for timestamped live bill-rules snapshots")
    ap.add_argument("--rules-ttl-seconds", type=int, default=21600, help="auto refresh ttl for latest rules snapshot")
    ap.add_argument("--manual-rules-snapshot", help="manual snapshot path with highest priority")
    ap.add_argument("--field-mapping", default=str(repo_path("references", "config", "field-mapping.xnofi.toml")))
    args = ap.parse_args()
    if bool(args.file) == bool(args.parsed):
        raise RuntimeError("exactly_one_of_file_or_parsed_required")

    workdir = Path(args.dir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    state_path = workdir / "workflow_state.json"

    # Resolve optional AI command.
    ai_cmd = args.ai_cmd or os.environ.get("ADV_QB_AI_CMD", "") or os.environ.get("OPENCLAW_AI_CMD", "")
    if not ai_cmd and args.ai_runtime_config and Path(args.ai_runtime_config).exists():
        try:
            arc = read_json(Path(args.ai_runtime_config))
            ai_cmd = str(arc.get("ai_cmd", "") or "").strip()
        except Exception:
            ai_cmd = ai_cmd
    ai_cmd = normalize_ai_cmd(str(ai_cmd or "").strip(), args.ai_runtime_config)
    require_ai = bool(args.require_ai or load_ai_runtime_required(args.ai_runtime_config))
    if require_ai and not ai_cmd:
        raise RuntimeError("require_ai_but_ai_cmd_missing")

    cache_root = Path(args.rules_cache_dir).resolve() if args.rules_cache_dir else (workdir / "rules_cache")

    state = {
        "state": "S1_PARSE_IDENTIFY",
        "inputs": {
            "file": str(Path(args.file).resolve()) if args.file else None,
            "parsed": str(Path(args.parsed).resolve()) if args.parsed else None,
            "bill_rules": str(Path(args.bill_rules).resolve()),
            "config": str(Path(args.config).resolve()) if args.config else None,
            "chunk_size": int(args.chunk_size),
            "max_batches_per_run": int(args.max_batches_per_run),
            "resume": bool(args.resume),
            "history": args.history,
            "ai_runtime_config": args.ai_runtime_config,
            "ai_cmd": ai_cmd,
            "require_ai": require_ai,
            "category_ai_mode": args.category_ai_mode,
            "batch_ai_review_mode": args.batch_ai_review_mode,
            "codex_review_mode": args.codex_review_mode,
            "manual_rules_snapshot": str(Path(args.manual_rules_snapshot).resolve()) if args.manual_rules_snapshot else None,
            "rules_cache_dir": str(cache_root),
            "field_mapping": str(Path(args.field_mapping).resolve()) if args.field_mapping else "",
        },
        "artifacts": {},
        "metrics": {},
        "flags": {
            "confirmed": bool(args.confirmed),
            "require_ai": require_ai,
            "require_codex_review": bool(args.require_codex_review),
        },
        "error": None,
    }

    try:
        # S1
        if args.parsed:
            parse_out = Path(args.parsed).resolve()
            parsed = read_json(parse_out)
            if not parsed.get("ok"):
                raise RuntimeError(f"parsed_input_not_ok:{parse_out}")
            state["state"] = "S2_MATCH_BUILD"
        else:
            parse_out = workdir / "parse_result.json"
            parsed = parse_uploaded_file(Path(args.file).resolve(), parse_out, state["inputs"].get("field_mapping") or "")
        state["artifacts"]["parse_result"] = str(parse_out)
        state["artifacts"]["field_mapping_file"] = str(Path(state["inputs"].get("field_mapping") or args.field_mapping).resolve())

        if maybe_run_chunk_job(args, workdir, state_path, state, parsed, parse_out, ai_cmd, require_ai):
            return

        write_json(state_path, state)

        # S2 (AI-judge facade + deterministic batch build)
        # priority: manual snapshot > cache latest > input bill-rules
        effective = resolve_effective_rules(Path(state["inputs"]["bill_rules"]), cache_root, state["inputs"].get("manual_rules_snapshot") or "")

        # TTL auto-refresh when using cached/latest path and config is available
        if args.config and effective["source"] in {"local_latest", "local_input"} and is_ttl_expired(cache_root / "latest.json", args.rules_ttl_seconds):
            try:
                refreshed = fetch_live_bill_rules(Path(args.config), cache_root)
                effective = {"path": refreshed["latest"], "source": "live_ttl_refresh", "hash": refreshed["hash"]}
                state["artifacts"]["live_bill_rules_snapshot"] = str(refreshed["snapshot"])
                state["artifacts"]["rules_manifest"] = str(refreshed["manifest"])
                state["artifacts"]["taxonomy_check"] = refreshed["taxonomy_check"]
            except Exception:
                # keep effective local rules if refresh fails
                pass

        ai_judge_out = workdir / "step2_ai_judge.json"
        use_per_record_ai = str(args.category_ai_mode or "per-record") == "per-record"
        category_ai_cmd = ai_cmd if use_per_record_ai else ""
        step2_require_ai = bool(require_ai and use_per_record_ai)
        step2_cmd = [
            "python", str(script_path("step2_match.py")),
            "--parsed", str(parse_out),
            "--bill-rules", str(effective["path"]),
            "--out", str(ai_judge_out),
        ]
        if category_ai_cmd:
            step2_cmd += ["--ai-cmd", category_ai_cmd]
        step2_cmd += ["--allow-fallback", "false" if step2_require_ai else "true"]
        if step2_require_ai:
            step2_cmd.append("--require-ai")
        if args.history:
            step2_cmd += ["--history", args.history]
        step2_started = time.perf_counter()
        run(step2_cmd)
        step2_elapsed_ms = round((time.perf_counter() - step2_started) * 1000.0, 2)
        state["artifacts"]["step2_ai_judge"] = str(ai_judge_out)
        try:
            step2_obj = read_json(ai_judge_out)
            state["metrics"]["category_judge"] = {
                "subprocess_duration_ms": step2_elapsed_ms,
                "mode": str(args.category_ai_mode),
                **(step2_obj.get("metrics") or {}),
            }
        except Exception:
            state["metrics"]["category_judge"] = {
                "subprocess_duration_ms": step2_elapsed_ms,
                "mode": str(args.category_ai_mode),
            }
        write_json(state_path, state)

        out_dir = workdir / "batch"
        build_ai_cmd = ai_cmd if use_per_record_ai else ""
        step2_build_cmd = [
            "python", str(script_path("step2_batch_build.py")),
            "--parsed", str(parse_out),
            "--bill-rules", str(effective["path"]),
            "--outDir", str(out_dir),
            "--chunk-size", str(max(1, args.chunk_size)),
            "--step2", str(ai_judge_out),
            "--rules-source", effective["source"],
            "--rules-snapshot", str(effective["path"]),
            "--rules-hash", effective["hash"],
            "--field-mapping", str(Path(state["inputs"].get("field_mapping") or args.field_mapping).resolve()),
        ]
        if build_ai_cmd:
            step2_build_cmd += ["--ai-cmd", build_ai_cmd]
        run(step2_build_cmd)
        summary = out_dir / "batch_match_summary.json"
        if not summary.exists():
            raise RuntimeError("batch_match_summary_missing")
        s = read_json(summary)

        # Cache-first strategy: only pull live rules when unresolved/low-confidence fields remain
        live_rules_path = None
        if args.config and needs_live_refresh(s):
            try:
                refreshed = fetch_live_bill_rules(Path(args.config), cache_root)
                live_rules_path = refreshed["snapshot"]
                run(step2_build_cmd + [
                    "--live-rules", str(live_rules_path),
                    "--rules-source", "live_retry",
                    "--rules-snapshot", str(live_rules_path),
                    "--rules-hash", refreshed["hash"],
                ])
                s = read_json(summary)
                state["artifacts"]["live_bill_rules_snapshot"] = str(live_rules_path)
                state["artifacts"]["rules_manifest"] = str(refreshed["manifest"])
                state["artifacts"]["taxonomy_check"] = refreshed["taxonomy_check"]
            except Exception:
                # keep local-cache results if live refresh fails
                live_rules_path = None

        # Merge Step2 AI category suggestions into batch match outputs
        run([
            "python", str(script_path("merge_step2_into_batch.py")),
            "--step2", str(ai_judge_out),
            "--summary", str(summary),
            "--auto-threshold", str(args.auto_threshold),
            "--confirm-threshold", str(args.confirm_threshold),
        ])
        s = read_json(summary)

        state["artifacts"]["batch_match_summary"] = str(summary)
        state["artifacts"]["effective_bill_rules"] = str(effective["path"])
        state["artifacts"]["effective_bill_rules_source"] = effective["source"]
        state["artifacts"]["effective_bill_rules_hash"] = effective["hash"]

        if str(args.category_ai_mode or "") == "batch-review":
            batch_ai_result = maybe_run_batch_category_ai_review(
                workdir=workdir,
                step2_path=ai_judge_out,
                summary_path=summary,
                bill_rules_path=Path(effective["path"]).resolve(),
                ai_cmd=ai_cmd,
                ai_runtime_config=args.ai_runtime_config,
                require_ai=require_ai,
                selection_mode=args.batch_ai_review_mode,
            )
            state["metrics"]["batch_category_ai_review"] = {
                key: value
                for key, value in batch_ai_result.items()
                if key not in {"payload_file", "response_file", "decisions_file", "audit_file", "queue_file"}
            }
            state["artifacts"]["batch_category_ai_review_queue_file"] = batch_ai_result["queue_file"]
            if batch_ai_result.get("payload_file"):
                state["artifacts"]["batch_category_ai_review_payload_file"] = batch_ai_result["payload_file"]
            if batch_ai_result.get("response_file"):
                state["artifacts"]["batch_category_ai_review_response_file"] = batch_ai_result["response_file"]
            if batch_ai_result.get("decisions_file"):
                state["artifacts"]["batch_category_ai_review_decisions_file"] = batch_ai_result["decisions_file"]
            if batch_ai_result.get("audit_file"):
                state["artifacts"]["batch_category_ai_review_audit_file"] = batch_ai_result["audit_file"]
            s = read_json(summary)
            write_json(state_path, state)

        codex_review_queue = workdir / "codex_review_queue.json"
        queue_obj = build_review_queue(ai_judge_out, summary, codex_review_queue, str(args.codex_review_mode))
        state["artifacts"]["codex_review_queue_file"] = str(codex_review_queue.resolve())
        state["metrics"]["codex_review_queue"] = queue_obj.get("counts") or {}

        if s.get("canonical_preview_file"):
            state["artifacts"]["canonical_preview_file"] = str(Path(s["canonical_preview_file"]).resolve())
        if s.get("canonical_ready_file"):
            state["artifacts"]["canonical_ready_file"] = str(Path(s["canonical_ready_file"]).resolve())
        if s.get("vendor_ai_audit_file"):
            state["artifacts"]["vendor_ai_audit_file"] = str(Path(s["vendor_ai_audit_file"]).resolve())
        if s.get("account_ai_review_log_file"):
            state["artifacts"]["account_ai_review_log_file"] = str(Path(s["account_ai_review_log_file"]).resolve())
        if s.get("account_ai_review_summary_file"):
            state["artifacts"]["account_ai_review_summary_file"] = str(Path(s["account_ai_review_summary_file"]).resolve())
        first_match = s.get("results", [{}])[0].get("match_file")
        if not first_match:
            raise RuntimeError("batch_first_match_missing")
        state["artifacts"]["first_match_result"] = str(Path(first_match).resolve())

        queue_count = int(((queue_obj.get("counts") or {}).get("queued_records")) or 0)
        if args.require_codex_review and queue_count > 0:
            state["state"] = "WAIT_CODEX_REVIEW"
            write_json(state_path, state)
            print(
                json.dumps(
                    {
                        "ok": True,
                        "state": state["state"],
                        "state_file": str(state_path),
                        "codex_review_queue_file": str(codex_review_queue.resolve()),
                        "codex_review_queue_count": queue_count,
                        "note": "codex_review_required_before_confirmation",
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        state["state"] = "S3_CONFIRM_RENDER"
        write_json(state_path, state)

        # S3 (python recap renderer)
        recap_out = workdir / "confirmation_recap.json"
        run([
            "python", str(script_path("step3_render_recap.py")),
            "--summary", str(summary),
            "--out", str(recap_out),
        ])
        state["artifacts"]["confirmation_recap"] = str(recap_out)
        state["state"] = "WAIT_CONFIRMATION"
        publish = plan_publish_targets(workdir)
        state["artifacts"]["published_run_name"] = publish["run_name"]
        state["artifacts"]["published_result_file"] = str(publish["result_file"].resolve())
        state["artifacts"]["published_issues_file"] = str(publish["issues_file"].resolve())
        state["artifacts"]["published_readable_issues_file"] = str(publish["issues_readable_file"].resolve())
        state["artifacts"]["published_ai_logs_dir"] = str(publish["ai_logs_root"].resolve())
        state["artifacts"]["published_intermediate_ai_dir"] = str(publish["intermediate_root"].resolve())
        write_json(state_path, state)
        published = publish_output_artifacts(workdir, state_path, recap_out, s)
        state["artifacts"]["published_result_file"] = published["result_file"]
        state["artifacts"]["issues_report_file"] = published["local_issues_file"]
        state["artifacts"]["readable_issues_report_file"] = published["local_readable_issues_file"]
        state["artifacts"]["local_ready_file"] = published["local_result_file"]
        state["artifacts"]["published_issues_file"] = published["issues_file"]
        state["artifacts"]["published_readable_issues_file"] = published["issues_readable_file"]
        state["artifacts"]["published_ai_log_file"] = published["ai_log_file"]
        state["artifacts"]["published_ai_summary_file"] = published["ai_summary_file"]
        state["artifacts"]["published_step2_ai_judge_file"] = published["step2_ai_judge_file"]
        if published.get("codex_review_queue_file"):
            state["artifacts"]["published_codex_review_queue_file"] = published["codex_review_queue_file"]
        if published.get("vendor_ai_audit_file"):
            state["artifacts"]["published_vendor_ai_audit_file"] = published["vendor_ai_audit_file"]
        state["artifacts"]["published_ai_logs_manifest"] = published["ai_logs_manifest"]
        write_json(state_path, state)

        out = {
            "ok": True,
            "state": state["state"],
            "state_file": str(state_path),
            "recap": str(recap_out),
            "output_only": True,
            "published_result_file": published["result_file"],
            "published_issues_file": published["issues_file"],
            "published_ai_logs_dir": published["ai_logs_root"],
            "published_ai_log_file": published["ai_log_file"],
            "published_intermediate_ai_dir": published["intermediate_ai_dir"],
        }
        if published.get("codex_review_queue_file"):
            out["published_codex_review_queue_file"] = published["codex_review_queue_file"]
        if (state.get("metrics") or {}).get("codex_review_queue"):
            out["codex_review_queue_count"] = (state["metrics"]["codex_review_queue"] or {}).get("queued_records", 0)
        if args.confirmed:
            out["note"] = "submit_disabled_in_output_only_mode"
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    except Exception as e:
        state["state"] = "ERROR"
        state["error"] = str(e)
        write_json(state_path, state)
        raise


if __name__ == "__main__":
    main()
