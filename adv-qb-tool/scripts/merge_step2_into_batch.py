#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


AUTHORITATIVE_AI_SOURCES = {"ai", "batch_reuse", "codex_review", "ai_batch_review"}


def read_json(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))


def write_json(p, obj):
    Path(p).write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def norm_text(value):
    return " ".join(str(value or "").strip().lower().split())


def resolve_near_summary(path_text, summary_dir, *, prefer_local=False):
    if not path_text:
        return summary_dir
    raw = Path(path_text)
    local = (summary_dir / raw.name).resolve()
    if prefer_local or local.exists():
        return local
    try:
        return raw.resolve()
    except Exception:
        return local


def candidate_from_field(field):
    return {
        "value": str(field.get("value") or "").strip(),
        "ref_id": str(field.get("ref_id") or "").strip(),
        "confidence": float(field.get("confidence") or 0),
        "alternatives": list(field.get("alternatives") or []),
        "source": str(field.get("source") or "").strip(),
        "needs_user_confirmation": bool(field.get("needs_user_confirmation")),
        "rule_reasons": list(field.get("rule_reasons") or []),
    }


def candidate_from_ai(ai):
    return {
        "value": str(ai.get("category_ref_text") or "").strip(),
        "ref_id": str(ai.get("category_ref_id") or "").strip(),
        "confidence": float(ai.get("confidence") or 0),
        "alternatives": list(ai.get("top3") or []),
        "source": str(ai.get("judge_source") or "").strip(),
        "rationale": str(ai.get("rationale") or "").strip(),
        "review_basis": ai.get("review_basis") or {},
        "reused_from_record_index": ai.get("reused_from_record_index"),
        "reuse_meta": ai.get("reuse_meta"),
        "duration_ms": ai.get("duration_ms"),
        "authoritative": bool(ai.get("authoritative")),
        "provider": str(ai.get("provider") or "").strip(),
        "fallback_used": bool(ai.get("fallback_used")),
    }


def set_business_line_account(payload, account_value, account_ref_id):
    if not isinstance(payload, dict):
        return
    for line in payload.get("lines") or []:
        if str(((line.get("meta") or {}).get("kind")) or "business") != "business":
            continue
        line["account_ref_text"] = account_value
        if account_ref_id:
            line["account_ref_id"] = account_ref_id
        else:
            line.pop("account_ref_id", None)


def refresh_status(match_obj):
    unresolved = [
        key
        for key, value in (match_obj.get("fields") or {}).items()
        if isinstance(value, dict) and value.get("needs_user_confirmation")
    ]
    unresolved += [f"source:{code}" for code in (match_obj.get("source_errors") or [])]
    unresolved += [f"validate:{code}" for code in (match_obj.get("validation_issues") or [])]
    match_obj.setdefault("interaction", {})["unresolved"] = unresolved
    if match_obj.get("source_errors"):
        match_obj["status"] = "invalid_source_data"
        match_obj["ready_to_upload"] = False
    elif unresolved or match_obj.get("validation_issues"):
        match_obj["status"] = "needs_user_confirmation"
        match_obj["ready_to_upload"] = False
    else:
        match_obj["status"] = "ready"
        match_obj["ready_to_upload"] = True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step2", required=True, help="step2_ai_judge.json")
    ap.add_argument("--summary", required=True, help="batch_match_summary.json")
    ap.add_argument("--auto-threshold", type=float, default=0.85)
    ap.add_argument("--confirm-threshold", type=float, default=0.65)
    args = ap.parse_args()

    step2 = read_json(args.step2)
    summary_path = Path(args.summary).resolve()
    summary_dir = summary_path.parent
    summary = read_json(summary_path)

    idx_to_step2 = {}
    for record in step2.get("records", []):
        idx_to_step2[int(record.get("record_index", 0))] = record

    audit_entries = []
    changed_count = 0
    authoritative_reviews = 0
    total_records = 0
    preview_bills = []
    ready_bills = []
    for item in summary.get("results", []):
        total_records += 1
        match_path = resolve_near_summary(item.get("match_file"), summary_dir)
        if not match_path:
            continue
        if not match_path.exists():
            continue

        item["match_file"] = str(match_path)
        match_obj = read_json(str(match_path))
        ridx = int(item.get("record_index", 0))
        step2_record = idx_to_step2.get(ridx) or {}
        ai = step2_record.get("category_ai") or {}
        account_field = match_obj.setdefault("fields", {}).setdefault("account_ref_text", {})
        category_field = match_obj.setdefault("fields", {}).setdefault("category_ref_text", {})

        code_candidate = account_field.get("code_candidate") or candidate_from_field(account_field)
        account_field["code_candidate"] = code_candidate
        category_field.setdefault("code_candidate", dict(code_candidate))

        ai_candidate = candidate_from_ai(ai)
        ai_source = ai_candidate.get("source", "")
        ai_authoritative = bool(ai_candidate.get("value")) and bool(ai_candidate.get("authoritative")) and ai_source in AUTHORITATIVE_AI_SOURCES
        if ai_authoritative:
            authoritative_reviews += 1

        code_value = code_candidate.get("value", "")
        ai_value = ai_candidate.get("value", "")
        changed = bool(ai_authoritative and ai_value and norm_text(ai_value) != norm_text(code_value))
        if changed:
            changed_count += 1

        if ai_authoritative and ai_value:
            final_candidate = {
                "value": ai_candidate.get("value", ""),
                "ref_id": ai_candidate.get("ref_id", ""),
                "confidence": ai_candidate.get("confidence", 0),
                "alternatives": ai_candidate.get("alternatives", []),
                "source": "ai_review_override" if changed else "ai_review_confirmed",
                "needs_user_confirmation": False,
            }
            account_field.update(final_candidate)
            account_field["selected_by"] = "ai_review_override" if changed else "ai_review_confirmed"
            account_field["ai_rationale"] = ai_candidate.get("rationale", "")
            account_field["ai_review"] = ai_candidate

            category_field.update(final_candidate)
            category_field["selected_by"] = account_field["selected_by"]
            category_field["ai_rationale"] = ai_candidate.get("rationale", "")
            category_field["ai_review"] = ai_candidate

            set_business_line_account(((match_obj.get("canonical_bill") or {}).get("payload") or {}), final_candidate["value"], final_candidate["ref_id"])
            set_business_line_account(((match_obj.get("collector_payload") or {}).get("payload") or {}), final_candidate["value"], final_candidate["ref_id"])
        else:
            account_field["selected_by"] = "code"
            category_field["selected_by"] = "code"
            if ai_candidate.get("value") or ai_source:
                account_field["ai_review"] = ai_candidate
                category_field["ai_review"] = ai_candidate

        match_obj["account_review"] = {
            "code_candidate": code_candidate,
            "ai_candidate": ai_candidate,
            "ai_authoritative": ai_authoritative,
            "changed": changed,
            "final_value": account_field.get("value", ""),
            "final_ref_id": account_field.get("ref_id", ""),
            "selected_by": account_field.get("selected_by", "code"),
        }
        refresh_status(match_obj)
        write_json(str(match_path), match_obj)
        preview_bills.append(match_obj.get("canonical_bill") or {})
        if match_obj.get("status") == "ready":
            ready_bills.append(match_obj.get("canonical_bill") or {})

        item["status"] = match_obj.get("status", item.get("status"))
        item["unresolved"] = (match_obj.get("interaction") or {}).get("unresolved", item.get("unresolved", []))
        item["source_errors"] = match_obj.get("source_errors", item.get("source_errors", []))
        item["validation_issues"] = match_obj.get("validation_issues", item.get("validation_issues", []))

        if changed:
            audit_entries.append(
                {
                    "record_index": ridx,
                    "request_no": ((step2_record.get("recap") or {}).get("request_no")),
                    "code_candidate": code_candidate,
                    "ai_candidate": ai_candidate,
                    "ai_authoritative": ai_authoritative,
                    "changed": changed,
                    "final_account": {
                        "value": account_field.get("value", ""),
                        "ref_id": account_field.get("ref_id", ""),
                        "selected_by": account_field.get("selected_by", "code"),
                    },
                    "ai_basis": {
                        "payment_detail_02_text": (((step2_record.get("recap") or {}).get("payment_detail_02_text")) or ""),
                        "reason": (((step2_record.get("recap") or {}).get("reason")) or ""),
                        "payment_detail_01_text": (((step2_record.get("recap") or {}).get("payment_detail_01_text")) or ""),
                        "review_basis": ai_candidate.get("review_basis") or {},
                        "review_request": (ai.get("review_request") or {}),
                        "rationale": ai_candidate.get("rationale", ""),
                        "top3": ai_candidate.get("alternatives", []),
                        "judge_source": ai_source,
                        "confidence": ai_candidate.get("confidence", 0),
                        "reused_from_record_index": ai_candidate.get("reused_from_record_index"),
                        "reuse_meta": ai_candidate.get("reuse_meta"),
                        "duration_ms": ai_candidate.get("duration_ms"),
                    },
                }
            )

    audit_dir = summary_dir
    audit_log_path = audit_dir / "account_ai_review_log.jsonl"
    with audit_log_path.open("w", encoding="utf-8") as fh:
        for entry in audit_entries:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    audit_summary = {
        "ok": True,
        "total_records": total_records,
        "authoritative_ai_reviews": authoritative_reviews,
        "changed_by_ai": changed_count,
        "unchanged_after_ai_review": max(0, authoritative_reviews - changed_count),
        "logged_difference_records": len(audit_entries),
        "log_file": str(audit_log_path.resolve()),
    }
    audit_summary_path = audit_dir / "account_ai_review_summary.json"
    write_json(audit_summary_path, audit_summary)

    preview_path = resolve_near_summary(summary.get("canonical_preview_file"), audit_dir, prefer_local=True)
    ready_path = resolve_near_summary(summary.get("canonical_ready_file"), audit_dir, prefer_local=True)
    write_json(preview_path, preview_bills)
    write_json(ready_path, ready_bills)

    summary["canonical_preview_file"] = str(preview_path)
    summary["canonical_ready_file"] = str(ready_path)
    summary["account_ai_review_log_file"] = str(audit_log_path.resolve())
    summary["account_ai_review_summary_file"] = str(audit_summary_path.resolve())
    summary["account_ai_review_changed_count"] = changed_count
    if summary.get("vendor_ai_audit_file"):
        summary["vendor_ai_audit_file"] = str(resolve_near_summary(summary.get("vendor_ai_audit_file"), audit_dir, prefer_local=True))
    write_json(summary_path, summary)
    print(
        json.dumps(
            {
                "ok": True,
                "summary": str(summary_path),
                "account_ai_review_log_file": str(audit_log_path.resolve()),
                "account_ai_review_summary_file": str(audit_summary_path.resolve()),
                "changed_by_ai": changed_count,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
