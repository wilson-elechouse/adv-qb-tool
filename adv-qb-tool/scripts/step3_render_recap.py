#!/usr/bin/env python3
import argparse, json
from pathlib import Path


def status_tag(v):
    if not isinstance(v, dict):
        return "resolved"
    if v.get("use_collector_default_when_empty"):
        return "default_fallback"
    return "needs_confirmation" if v.get("needs_user_confirmation") else "resolved"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    summary = json.loads(Path(args.summary).read_text(encoding="utf-8"))
    records = []
    for item in summary.get("results", []):
        m = json.loads(Path(item["match_file"]).read_text(encoding="utf-8"))
        f = m.get("fields", {})
        rec = {
            "record_index": item.get("record_index"),
            "confirmation_required": m.get("status") != "ready",
            "kind": m.get("kind"),
            "status": m.get("status"),
            "bill_rules_source": m.get("bill_rules_source", "local"),
            "fields": {
                "vendor": {**(f.get("vendor_ref_text") or {}), "status": status_tag(f.get("vendor_ref_text"))},
                "bill_number": {"value": (f.get("bill_no") or {}).get("value", ""), "status": "resolved"},
                "request_no": {"value": (f.get("client_ref") or {}).get("value", ""), "status": "resolved"},
                "bill_date": {"value": (f.get("bill_date") or {}).get("value", ""), "status": "resolved"},
                "due_date": {"value": (f.get("due_date") or {}).get("value", ""), "status": status_tag(f.get("due_date"))},
                "location": {**(f.get("location_ref_text") or {}), "status": status_tag(f.get("location_ref_text"))},
                "category": {**(f.get("category_ref_text") or {}), "status": status_tag(f.get("category_ref_text"))},
                "tax": {**((f.get("tax_ref_text") or f.get("tax_code_ref_text")) or {}), "status": status_tag((f.get("tax_ref_text") or f.get("tax_code_ref_text")) or {})},
                "withholding_tax": {**(f.get("withholding_tax") or {}), "status": status_tag(f.get("withholding_tax"))},
            },
            "unresolved": (m.get("interaction") or {}).get("unresolved", []),
            "source_errors": m.get("source_errors") or [],
            "validation_issues": m.get("validation_issues") or [],
        }
        records.append(rec)

    out = {"ok": True, "mode": "batch", "total_records": len(records), "records": records, "next_action": "wait_for_user_confirmation"}
    p = Path(args.out)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "out": str(p.resolve()), "total_records": len(records)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
