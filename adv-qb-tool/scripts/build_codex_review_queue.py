#!/usr/bin/env python3
import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

from canonical_support import read_json, write_json


DEFAULT_WEAK_SOURCES = {"", "rule_fallback", "rule_payment_type"}


def candidate_from_field(field: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "value": str(field.get("value") or "").strip(),
        "ref_id": str(field.get("ref_id") or "").strip(),
        "confidence": float(field.get("confidence") or 0),
        "alternatives": list(field.get("alternatives") or []),
        "source": str(field.get("source") or "").strip(),
        "needs_user_confirmation": bool(field.get("needs_user_confirmation")),
        "rule_reasons": list(field.get("rule_reasons") or []),
        "selected_by": str(field.get("selected_by") or "").strip(),
        "ai_rationale": str(field.get("ai_rationale") or "").strip(),
    }


def candidate_from_ai(ai: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "value": str(ai.get("category_ref_text") or "").strip(),
        "ref_id": str(ai.get("category_ref_id") or "").strip(),
        "confidence": float(ai.get("confidence") or 0),
        "alternatives": list(ai.get("top3") or []),
        "source": str(ai.get("judge_source") or "").strip(),
        "rationale": str(ai.get("rationale") or "").strip(),
        "authoritative": bool(ai.get("authoritative")),
        "provider": str(ai.get("provider") or "").strip(),
        "fallback_used": bool(ai.get("fallback_used")),
        "reused_from_record_index": ai.get("reused_from_record_index"),
        "reuse_meta": ai.get("reuse_meta"),
    }


def parse_csv_set(value: str, *, defaults: Iterable[str]) -> set[str]:
    raw = str(value or "").strip()
    if not raw:
        return {str(item or "").strip() for item in defaults}
    return {part.strip() for part in raw.split(",")}


def should_queue(
    *,
    status: str,
    field: Dict[str, Any],
    ai: Dict[str, Any],
    min_confidence: float,
    weak_sources: set[str],
    mode: str,
) -> tuple[bool, List[str]]:
    reasons: List[str] = []

    ai_source = str(ai.get("judge_source") or "").strip() or "missing"
    if bool(ai.get("authoritative")):
        return False, [f"already_reviewed:{ai_source}"]
    if not bool(ai.get("authoritative")):
        reasons.append(f"non_authoritative_ai:{ai_source}")

    source = str(field.get("source") or "").strip()
    if source in weak_sources:
        reasons.append(f"weak_code_source:{source or 'empty'}")

    confidence = float(field.get("confidence") or 0)
    if confidence < float(min_confidence):
        reasons.append(f"low_code_confidence:{round(confidence, 4)}")

    if bool(field.get("needs_user_confirmation")):
        reasons.append("needs_user_confirmation")

    if status and status != "ready":
        reasons.append(f"status:{status}")

    if mode == "all-eligible":
        return True, reasons

    actionable = [
        reason
        for reason in reasons
        if reason.startswith("weak_code_source:")
        or reason.startswith("low_code_confidence:")
        or reason == "needs_user_confirmation"
        or reason.startswith("status:needs_user_confirmation")
    ]
    return bool(actionable), reasons


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step2", required=True, help="step2_ai_judge.json")
    ap.add_argument("--summary", required=True, help="batch_match_summary.json")
    ap.add_argument("--out", required=True, help="output review queue file")
    ap.add_argument("--mode", choices=["review-worthy", "all-eligible"], default="review-worthy")
    ap.add_argument("--min-confidence", type=float, default=0.85)
    ap.add_argument("--weak-sources", default="rule_fallback,rule_payment_type,")
    ap.add_argument("--include-invalid-source", choices=["true", "false"], default="false")
    args = ap.parse_args()

    include_invalid_source = str(args.include_invalid_source).lower() == "true"
    weak_sources = parse_csv_set(args.weak_sources, defaults=DEFAULT_WEAK_SOURCES)

    step2_path = Path(args.step2).resolve()
    summary_path = Path(args.summary).resolve()
    out_path = Path(args.out).resolve()

    step2 = read_json(step2_path)
    summary = read_json(summary_path)

    step2_index: Dict[int, Dict[str, Any]] = {}
    for record in step2.get("records", []):
        try:
            step2_index[int(record.get("record_index", 0))] = record
        except Exception:
            continue

    review_reason_counts: Counter[str] = Counter()
    stats = {
        "summary_records": len(summary.get("results", [])),
        "eligible_records": 0,
        "queued_records": 0,
        "skipped_missing_match_file": 0,
        "skipped_missing_step2_record": 0,
        "skipped_missing_review_request": 0,
        "skipped_invalid_source_status": 0,
        "skipped_not_review_worthy": 0,
    }

    queued_records: List[Dict[str, Any]] = []
    for item in summary.get("results", []):
        match_path = Path(str(item.get("match_file") or "")).resolve()
        if not match_path.exists():
            stats["skipped_missing_match_file"] += 1
            continue

        try:
            record_index = int(item.get("record_index", 0))
        except Exception:
            record_index = 0
        step2_record = step2_index.get(record_index)
        if not step2_record:
            stats["skipped_missing_step2_record"] += 1
            continue

        match_obj = read_json(match_path)
        status = str(item.get("status") or match_obj.get("status") or "").strip()
        if status == "invalid_source_data" and not include_invalid_source:
            stats["skipped_invalid_source_status"] += 1
            continue

        ai = step2_record.get("category_ai") or {}
        review_request = ai.get("review_request") or (ai.get("review_basis") or {}).get("review_request") or {}
        allowed_options = list(review_request.get("allowed_options") or [])
        if not allowed_options:
            stats["skipped_missing_review_request"] += 1
            continue

        fields = match_obj.get("fields") or {}
        account_field = fields.get("account_ref_text") or {}
        code_candidate = account_field.get("code_candidate") or candidate_from_field(account_field)
        include, review_reasons = should_queue(
            status=status,
            field=code_candidate,
            ai=ai,
            min_confidence=args.min_confidence,
            weak_sources=weak_sources,
            mode=args.mode,
        )
        stats["eligible_records"] += 1
        if not include:
            stats["skipped_not_review_worthy"] += 1
            continue

        for reason in review_reasons:
            review_reason_counts[reason] += 1

        recap = step2_record.get("recap") or {}
        ai_candidate = candidate_from_ai(ai)
        queue_record = {
            "queue_index": len(queued_records) + 1,
            "record_index": record_index,
            "row_number": step2_record.get("row_number"),
            "request_no": str(recap.get("request_no") or ""),
            "recap_summary": {
                "vendor": str(recap.get("vendor") or ""),
                "payment_type": str(recap.get("payment_type") or ""),
                "product": str(recap.get("product") or ""),
                "which_client": str(recap.get("which_client") or ""),
                "bill_number": str(recap.get("bill_number") or ""),
                "gross_amount": recap.get("gross_amount"),
                "bill_date": str(recap.get("bill_date") or ""),
                "due_date": str(recap.get("due_date") or ""),
            },
            "status": status,
            "match_file": str(match_path),
            "review_reasons": review_reasons,
            "current_account": code_candidate,
            "current_ai": ai_candidate,
            "source_errors": list(item.get("source_errors") or match_obj.get("source_errors") or []),
            "validation_issues": list(item.get("validation_issues") or match_obj.get("validation_issues") or []),
            "unresolved": list(item.get("unresolved") or ((match_obj.get("interaction") or {}).get("unresolved") or [])),
            "review_request": review_request,
            "review_basis": ai.get("review_basis") or {},
            "decision_stub": {
                "record_index": record_index,
                "category_ref_text": str(code_candidate.get("value") or ""),
                "confidence": float(code_candidate.get("confidence") or 0),
                "top3": list(code_candidate.get("alternatives") or [])[:3],
                "rationale": "",
            },
        }
        queued_records.append(queue_record)

    stats["queued_records"] = len(queued_records)
    out_obj = {
        "ok": True,
        "mode": "codex_review_queue",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "step2_file": str(step2_path),
        "summary_file": str(summary_path),
        "selection": {
            "mode": args.mode,
            "min_confidence": args.min_confidence,
            "weak_sources": sorted(weak_sources),
            "include_invalid_source": include_invalid_source,
        },
        "counts": {
            **stats,
            "review_reason_counts": dict(sorted(review_reason_counts.items())),
        },
        "decision_schema": {
            "records": [
                {
                    "record_index": 0,
                    "category_ref_text": "5702 Consultancy Fee",
                    "confidence": 0.94,
                    "top3": [
                        "5702 Consultancy Fee",
                        "5701 Bookkeeping Services",
                        "Legal and professional fees",
                    ],
                    "rationale": "Choose the category that best matches the accounting substance.",
                }
            ]
        },
        "records": queued_records,
    }
    write_json(out_path, out_obj)
    print(
        json.dumps(
            {
                "ok": True,
                "queue_file": str(out_path),
                "queued_records": len(queued_records),
                "eligible_records": stats["eligible_records"],
                "mode": args.mode,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
