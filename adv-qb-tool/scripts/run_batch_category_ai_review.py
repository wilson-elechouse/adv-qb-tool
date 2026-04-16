#!/usr/bin/env python3
import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List

from apply_codex_review_decisions import extract_decisions
from canonical_support import read_json, write_json


def load_runtime_config(path_text: str) -> Dict[str, Any]:
    path = Path(str(path_text or "").strip())
    if not path.exists():
        return {}
    try:
        return read_json(path)
    except Exception:
        return {}


def truncate_text(value: Any, max_chars: int) -> str:
    text = " ".join(str(value or "").split())
    limit = max(0, int(max_chars or 0))
    if limit <= 0 or len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3] + "..."


def trim_options(options: List[Any], max_chars: int) -> List[str]:
    out: List[str] = []
    for item in options or []:
        label = truncate_text(item, max_chars)
        if label and label not in out:
            out.append(label)
    return out


def compact_candidate(candidate: Dict[str, Any], option_chars: int) -> Dict[str, Any]:
    return {
        "value": str(candidate.get("value") or "").strip(),
        "confidence": float(candidate.get("confidence") or 0),
        "source": str(candidate.get("source") or "").strip(),
        "alternatives": trim_options(list(candidate.get("alternatives") or []), option_chars)[:3],
        "rule_reasons": list(candidate.get("rule_reasons") or []),
        "needs_user_confirmation": bool(candidate.get("needs_user_confirmation")),
    }


def compact_record(record: Dict[str, Any], limits: Dict[str, int], shared_options: List[str] | None) -> Dict[str, Any]:
    review_basis = record.get("review_basis") or {}
    primary = review_basis.get("primary") or {}
    supporting = review_basis.get("supporting") or {}
    feature_summary = review_basis.get("feature_summary") or {}
    recap_summary = record.get("recap_summary") or {}
    payload = {
        "record_index": record.get("record_index"),
        "row_number": record.get("row_number"),
        "request_no": str(record.get("request_no") or ""),
        "status": str(record.get("status") or ""),
        "review_reasons": list(record.get("review_reasons") or []),
        "recap_summary": {
            "vendor": str(recap_summary.get("vendor") or ""),
            "payment_type": str(recap_summary.get("payment_type") or ""),
            "product": str(recap_summary.get("product") or ""),
            "which_client": str(recap_summary.get("which_client") or ""),
            "bill_number": str(recap_summary.get("bill_number") or ""),
            "gross_amount": recap_summary.get("gross_amount"),
            "bill_date": str(recap_summary.get("bill_date") or ""),
            "due_date": str(recap_summary.get("due_date") or ""),
        },
        "source_inputs": {
            "payment_detail_01_text": truncate_text(supporting.get("payment_detail_01_text"), limits["pd01"]),
            "payment_detail_02_text": truncate_text(primary.get("payment_detail_02_text"), limits["pd02"]),
            "reason": truncate_text(primary.get("reason"), limits["reason"]),
        },
        "feature_summary": {
            "payment_type": str(feature_summary.get("payment_type") or ""),
            "supplier": str(feature_summary.get("supplier") or ""),
            "product": str(feature_summary.get("product") or ""),
            "payment_to": str(feature_summary.get("payment_to") or ""),
            "reason_signature": truncate_text(feature_summary.get("reason_signature"), limits["reason"]),
        },
        "program_result": {
            "current_account": compact_candidate(record.get("current_account") or {}, limits["option_label"]),
            "unresolved": list(record.get("unresolved") or []),
            "validation_issues": list(record.get("validation_issues") or []),
            "source_errors": list(record.get("source_errors") or []),
        },
    }
    if shared_options is None:
        payload["allowed_options"] = trim_options(
            list(((record.get("review_request") or {}).get("allowed_options") or [])),
            limits["option_label"],
        )
    return payload


def parse_json_output(text: str) -> Any:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        pass
    for line in reversed([item for item in raw.splitlines() if item.strip()]):
        try:
            return json.loads(line)
        except Exception:
            continue
    raise ValueError("batch_ai_invalid_json_output")


def validate_decisions(records: List[Dict[str, Any]], decisions: List[Dict[str, Any]]) -> Dict[str, Any]:
    expected_indexes = []
    for item in records:
        try:
            expected_indexes.append(int(item.get("record_index")))
        except Exception:
            continue
    expected_set = set(expected_indexes)
    seen: set[int] = set()
    valid: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for item in decisions:
        try:
            record_index = int(item.get("record_index"))
        except Exception:
            skipped.append({"decision": item, "reason": "missing_record_index"})
            continue
        if record_index not in expected_set:
            skipped.append({"record_index": record_index, "reason": "record_not_in_queue"})
            continue
        if record_index in seen:
            skipped.append({"record_index": record_index, "reason": "duplicate_record_index"})
            continue
        choice = str(item.get("category_ref_text") or item.get("choice") or "").strip()
        if not choice:
            skipped.append({"record_index": record_index, "reason": "missing_category_ref_text"})
            continue
        seen.add(record_index)
        valid.append(item)
    missing = [idx for idx in expected_indexes if idx not in seen]
    return {
        "expected_indexes": expected_indexes,
        "valid": valid,
        "skipped": skipped,
        "missing": missing,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", required=True, help="batch AI review queue json")
    ap.add_argument("--ai-cmd", required=True, help="shell command that accepts strict JSON on stdin")
    ap.add_argument("--out", required=True, help="output decisions json")
    ap.add_argument("--payload-out", help="optional saved request payload")
    ap.add_argument("--response-out", help="optional saved raw AI response")
    ap.add_argument("--audit-out", help="optional audit summary json")
    ap.add_argument("--runtime-config", default="")
    ap.add_argument("--require-complete", choices=["true", "false"], default="false")
    args = ap.parse_args()

    queue_path = Path(args.queue).resolve()
    out_path = Path(args.out).resolve()
    payload_out = Path(args.payload_out).resolve() if args.payload_out else None
    response_out = Path(args.response_out).resolve() if args.response_out else None
    audit_out = Path(args.audit_out).resolve() if args.audit_out else None

    queue = read_json(queue_path)
    queued_records = list(queue.get("records") or [])
    runtime = load_runtime_config(args.runtime_config)
    limits = {
        "pd01": int(runtime.get("pd01_max_chars", 240) or 240),
        "pd02": int(runtime.get("pd02_max_chars", 320) or 320),
        "reason": int(runtime.get("reason_max_chars", 220) or 220),
        "option_label": int(runtime.get("option_label_max_chars", 72) or 72),
    }

    shared_options = None
    option_sets = []
    for item in queued_records:
        option_sets.append(tuple((item.get("review_request") or {}).get("allowed_options") or []))
    if option_sets and all(option_sets[0] == option_set for option_set in option_sets[1:]):
        shared_options = trim_options(list(option_sets[0]), limits["option_label"])

    payload = {
        "task": "batch_category_review",
        "instruction": (
            "Review each record using payment_detail_02_text and reason as primary evidence, "
            "payment_detail_01_text as supporting evidence, and the current code-selected account as the baseline. "
            "Return strict JSON only with records:[{record_index, category_ref_text, confidence, top3, rationale}]. "
            "Choose exactly one allowed account per record."
        ),
        "selection": queue.get("selection") or {},
        "decision_schema": queue.get("decision_schema") or {
            "records": [
                {
                    "record_index": 0,
                    "category_ref_text": "5702 Consultancy Fee",
                    "confidence": 0.94,
                    "top3": ["5702 Consultancy Fee"],
                    "rationale": "Best accounting fit for the payment details.",
                }
            ]
        },
        "shared_allowed_options": shared_options,
        "records": [compact_record(item, limits, shared_options) for item in queued_records],
    }
    if payload_out:
        write_json(payload_out, payload)

    started = time.perf_counter()
    proc = subprocess.run(
        args.ai_cmd,
        input=json.dumps(payload, ensure_ascii=False),
        text=True,
        capture_output=True,
        shell=True,
        timeout=max(1, int(runtime.get("timeout_seconds", 180) or 180)),
    )
    duration_ms = round((time.perf_counter() - started) * 1000.0, 2)
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if response_out:
        response_out.parent.mkdir(parents=True, exist_ok=True)
        response_out.write_text(stdout, encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(f"batch_ai_cmd_nonzero:{proc.returncode}:{stderr}")

    obj = parse_json_output(stdout)
    decisions = extract_decisions(obj)
    validation = validate_decisions(queued_records, decisions)
    if str(args.require_complete).lower() == "true" and validation["missing"]:
        raise RuntimeError(
            "batch_ai_incomplete:" + ",".join(str(item) for item in validation["missing"][:20])
        )

    out_payload = {"records": validation["valid"]}
    write_json(out_path, out_payload)

    audit_payload = {
        "ok": True,
        "queue_file": str(queue_path),
        "decisions_file": str(out_path),
        "records_submitted": len(queued_records),
        "decisions_received": len(decisions),
        "decisions_applied": len(validation["valid"]),
        "missing_record_indexes": validation["missing"],
        "skipped_decisions": validation["skipped"],
        "duration_ms": duration_ms,
        "shared_allowed_options_count": len(shared_options or []),
        "ai_cmd": args.ai_cmd,
        "response_root_type": type(obj).__name__,
    }
    if audit_out:
        write_json(audit_out, audit_payload)

    print(
        json.dumps(
            {
                "ok": True,
                "decisions_file": str(out_path),
                "records_submitted": len(queued_records),
                "decisions_applied": len(validation["valid"]),
                "missing_record_indexes": validation["missing"],
                "duration_ms": duration_ms,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
