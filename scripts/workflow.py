#!/usr/bin/env python3
import argparse
import copy
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path



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


def _sha256_file(p: Path):
    return hashlib.sha256(p.read_text(encoding="utf-8").encode("utf-8")).hexdigest()


def fetch_live_bill_rules(config_path: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    raw = run([
        "node", "skills/adv-qbo-tool/scripts/refresh_bill_rules.mjs",
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
        if unresolved.intersection({"vendor_ref_text", "category_ref_text", "tax_code_ref_text", "location_ref_text"}):
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


def parse_uploaded_file(file_path: Path, out_path: Path):
    run([
        "python", "skills/adv-qbo-tool/scripts/parse_payment_request_xlsx.py",
        "--file", str(file_path),
        "--out", str(out_path),
    ])
    parsed = read_json(out_path)
    if not parsed.get("ok"):
        raise RuntimeError(f"parse_failed:{parsed.get('error','unknown')}")
    return parsed


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
        for item in [*success_items, *failed_items, *pending_items]
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
        "retry_hint": "python skills/adv-qbo-tool/scripts/retry_failed_chunk_job.py",
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


def maybe_run_chunk_job(args, workdir: Path, state_path: Path, state, parsed, parse_out: Path, ai_cmd: str):
    records = parsed.get("records") or []
    chunk_size = max(1, int(args.chunk_size))
    if args.parsed or len(records) <= chunk_size:
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
    state["flags"]["confirmed"] = bool(args.confirmed)
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
    state["state"] = "DONE" if args.confirmed else "WAIT_CONFIRMATION"
    if completed_batches < total_batches:
        state["state"] = "WAIT_NEXT_BATCH"
    write_json(state_path, state)
    summary = write_chunk_job_summary(summary_path, state)
    report = build_chunk_job_report(report_path, state, parsed)
    last_batch = summary.get("last_completed_batch") or {}

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
    ap.add_argument("--confirmed", action="store_true")
    ap.add_argument("--config")
    ap.add_argument("--ai-cmd", help="AI command for Step2 category judge (optional if configured in ai-runtime.json or OPENCLAW_AI_CMD)")
    ap.add_argument("--ai-runtime-config", default="skills/adv-qbo-tool/references/config/ai-runtime.json")
    ap.add_argument("--history", help="optional confirmed category history json")
    ap.add_argument("--auto-threshold", type=float, default=0.85)
    ap.add_argument("--confirm-threshold", type=float, default=0.65)
    ap.add_argument("--rules-cache-dir", help="optional directory for timestamped live bill-rules snapshots")
    ap.add_argument("--rules-ttl-seconds", type=int, default=21600, help="auto refresh ttl for latest rules snapshot")
    ap.add_argument("--manual-rules-snapshot", help="manual snapshot path with highest priority")
    args = ap.parse_args()
    if bool(args.file) == bool(args.parsed):
        raise RuntimeError("exactly_one_of_file_or_parsed_required")

    workdir = Path(args.dir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    state_path = workdir / "workflow_state.json"

    # Resolve mandatory AI command (AI required, no fallback)
    ai_cmd = args.ai_cmd or os.environ.get("OPENCLAW_AI_CMD", "")
    if not ai_cmd and args.ai_runtime_config and Path(args.ai_runtime_config).exists():
        try:
            arc = read_json(Path(args.ai_runtime_config))
            ai_cmd = str(arc.get("ai_cmd", "") or "").strip()
        except Exception:
            ai_cmd = ai_cmd
    if not ai_cmd:
        ai_cmd = "python skills/adv-qbo-tool/scripts/ai_bridge.py"
    if not ai_cmd:
        raise RuntimeError("ai_required_missing: configure --ai-cmd or OPENCLAW_AI_CMD or references/config/ai-runtime.json:ai_cmd")

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
            "manual_rules_snapshot": str(Path(args.manual_rules_snapshot).resolve()) if args.manual_rules_snapshot else None,
            "rules_cache_dir": str(cache_root),
        },
        "artifacts": {},
        "metrics": {},
        "flags": {"confirmed": bool(args.confirmed)},
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
            parsed = parse_uploaded_file(Path(args.file).resolve(), parse_out)
        state["artifacts"]["parse_result"] = str(parse_out)

        if maybe_run_chunk_job(args, workdir, state_path, state, parsed, parse_out, ai_cmd):
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
        step2_cmd = [
            "python", "skills/adv-qbo-tool/scripts/step2_match.py",
            "--parsed", str(parse_out),
            "--bill-rules", str(effective["path"]),
            "--out", str(ai_judge_out),
        ]
        step2_cmd += ["--ai-cmd", ai_cmd, "--allow-fallback", "false"]
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
                **(step2_obj.get("metrics") or {}),
            }
        except Exception:
            state["metrics"]["category_judge"] = {"subprocess_duration_ms": step2_elapsed_ms}
        write_json(state_path, state)

        out_dir = workdir / "batch"
        step2_build_cmd = [
            "python", "skills/adv-qbo-tool/scripts/step2_batch_build.py",
            "--parsed", str(parse_out),
            "--bill-rules", str(effective["path"]),
            "--outDir", str(out_dir),
            "--chunk-size", str(max(1, args.chunk_size)),
            "--step2", str(ai_judge_out),
            "--rules-source", effective["source"],
            "--rules-snapshot", str(effective["path"]),
            "--rules-hash", effective["hash"],
        ]
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
            "python", "skills/adv-qbo-tool/scripts/merge_step2_into_batch.py",
            "--step2", str(ai_judge_out),
            "--summary", str(summary),
            "--auto-threshold", str(args.auto_threshold),
            "--confirm-threshold", str(args.confirm_threshold),
        ])

        state["artifacts"]["batch_match_summary"] = str(summary)
        state["artifacts"]["effective_bill_rules"] = str(effective["path"])
        state["artifacts"]["effective_bill_rules_source"] = effective["source"]
        state["artifacts"]["effective_bill_rules_hash"] = effective["hash"]
        first_match = s.get("results", [{}])[0].get("match_file")
        if not first_match:
            raise RuntimeError("batch_first_match_missing")
        state["artifacts"]["first_match_result"] = str(Path(first_match).resolve())
        state["state"] = "S3_CONFIRM_RENDER"
        write_json(state_path, state)

        # S3 (python recap renderer)
        recap_out = workdir / "confirmation_recap.json"
        run([
            "python", "skills/adv-qbo-tool/scripts/step3_render_recap.py",
            "--summary", str(summary),
            "--out", str(recap_out),
        ])
        state["artifacts"]["confirmation_recap"] = str(recap_out)
        state["state"] = "WAIT_CONFIRMATION"
        write_json(state_path, state)

        # Stop at confirmation unless explicitly confirmed.
        if not args.confirmed:
            print(json.dumps({
                "ok": True,
                "state": state["state"],
                "state_file": str(state_path),
                "recap": str(recap_out),
            }, ensure_ascii=False, indent=2))
            return

        # S4/S5: create -> validate -> precheck submit chain
        if not args.config:
            raise RuntimeError("confirmed_submit_requires_config")
        state["state"] = "S4_SUBMIT_PRECHECK"
        write_json(state_path, state)

        submit_raw = run([
            "python", "skills/adv-qbo-tool/scripts/step4_submit.py",
            "--summary", str(summary),
            "--config", str(Path(args.config).resolve()),
            "--confirmation-received", "true",
            "--on-client-ref-conflict", "block",
        ])
        submit_obj = parse_json_output(submit_raw)
        submit_out = submit_obj.get("out")
        if submit_out:
            state["artifacts"]["batch_submit_result"] = str(Path(submit_out).resolve())

        state["state"] = "DONE"
        write_json(state_path, state)

        print(json.dumps({
            "ok": True,
            "state": state["state"],
            "state_file": str(state_path),
            "recap": str(recap_out),
            "submit_result": state["artifacts"].get("batch_submit_result"),
            "submit_summary": submit_obj or None,
        }, ensure_ascii=False, indent=2))

    except Exception as e:
        state["state"] = "ERROR"
        state["error"] = str(e)
        write_json(state_path, state)
        raise


if __name__ == "__main__":
    main()
