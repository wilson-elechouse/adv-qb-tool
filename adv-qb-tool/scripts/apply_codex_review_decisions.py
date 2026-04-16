#!/usr/bin/env python3
import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from canonical_support import load_bill_rules, match_option, read_json, write_json


def extract_decisions(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    if isinstance(payload.get("decisions"), list):
        return [item for item in payload.get("decisions", []) if isinstance(item, dict)]

    records = payload.get("records")
    if not isinstance(records, list):
        return []

    extracted: List[Dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        if isinstance(item.get("decision"), dict):
            decision = dict(item.get("decision") or {})
            decision.setdefault("record_index", item.get("record_index"))
            extracted.append(decision)
            continue
        if "category_ref_text" in item or "choice" in item:
            extracted.append(item)
    return extracted


def normalize_top3(choice_text: str, top3: List[Any], account_options: List[Dict[str, str]]) -> List[str]:
    labels: List[str] = []
    for value in [choice_text, *(top3 or [])]:
        label = str(value or "").strip()
        if not label:
            continue
        matched = match_option(label, account_options)
        canonical = str(matched.get("value") or label).strip()
        if canonical and canonical not in labels:
            labels.append(canonical)
        if len(labels) >= 3:
            break
    return labels


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step2", required=True, help="input step2_ai_judge.json")
    ap.add_argument("--decisions", required=True, help="decision file or edited review queue")
    ap.add_argument("--bill-rules", required=True, help="bill-rules json for account ID resolution")
    ap.add_argument("--out", help="output step2_ai_judge.json; defaults to updating --step2 in place")
    ap.add_argument("--judge-source", default="codex_review")
    ap.add_argument("--provider", default="codex_review")
    ap.add_argument("--review-mode", default="codex_manual")
    args = ap.parse_args()

    step2_path = Path(args.step2).resolve()
    decisions_path = Path(args.decisions).resolve()
    out_path = Path(args.out).resolve() if args.out else step2_path

    step2 = read_json(step2_path)
    decision_payload = read_json(decisions_path)
    bill_rules = load_bill_rules(args.bill_rules)
    account_options = bill_rules.get("accounts") or []
    decisions = extract_decisions(decision_payload)

    record_index_to_record: Dict[int, Dict[str, Any]] = {}
    for record in step2.get("records", []):
        try:
            record_index_to_record[int(record.get("record_index", 0))] = record
        except Exception:
            continue

    seen_indexes: set[int] = set()
    applied = 0
    skipped = []
    for decision in decisions:
        try:
            record_index = int(decision.get("record_index"))
        except Exception:
            skipped.append({"decision": decision, "reason": "missing_record_index"})
            continue
        if record_index in seen_indexes:
            skipped.append({"record_index": record_index, "reason": "duplicate_record_index"})
            continue
        seen_indexes.add(record_index)

        record = record_index_to_record.get(record_index)
        if not record:
            skipped.append({"record_index": record_index, "reason": "record_not_found"})
            continue

        choice_text = str(decision.get("category_ref_text") or decision.get("choice") or "").strip()
        if not choice_text:
            skipped.append({"record_index": record_index, "reason": "missing_category_ref_text"})
            continue

        matched = match_option(choice_text, account_options)
        resolved_value = str(matched.get("value") or "").strip()
        if not resolved_value:
            skipped.append({"record_index": record_index, "reason": "choice_not_resolved", "choice": choice_text})
            continue

        existing_ai = record.get("category_ai") or {}
        review_request = existing_ai.get("review_request") or (existing_ai.get("review_basis") or {}).get("review_request") or {}
        review_basis = dict(existing_ai.get("review_basis") or {})
        review_basis["reviewed_at"] = datetime.now().isoformat(timespec="seconds")
        review_basis["review_mode"] = str(args.review_mode or "codex_manual")
        review_basis["decision_source_file"] = str(decisions_path)
        review_basis["review_provider"] = str(decision.get("provider") or args.provider or "")

        top3 = normalize_top3(
            resolved_value,
            list(decision.get("top3") or []),
            account_options,
        )
        record["category_ai"] = {
            "category_ref_text": resolved_value,
            "category_ref_id": str(matched.get("ref_id") or ""),
            "confidence": float(decision.get("confidence", matched.get("score", 0)) or 0),
            "top3": top3 or list(matched.get("alternatives") or [])[:3],
            "rationale": str(decision.get("rationale") or "codex_review"),
            "review_basis": review_basis,
            "review_request": review_request,
            "history_examples_used": len(list(review_request.get("history_examples") or [])),
            "judge_source": str(args.judge_source or "codex_review"),
            "authoritative": bool(decision.get("authoritative", True)),
            "provider": str(decision.get("provider") or args.provider or "codex_review"),
            "fallback_used": False,
            "reused_from_record_index": None,
            "reuse_meta": None,
            "duration_ms": decision.get("duration_ms"),
        }
        applied += 1

    write_json(out_path, step2)
    print(
        json.dumps(
            {
                "ok": True,
                "step2_file": str(out_path),
                "decisions_file": str(decisions_path),
                "applied": applied,
                "skipped": len(skipped),
                "skipped_items": skipped,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
