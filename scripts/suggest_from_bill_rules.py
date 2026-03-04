#!/usr/bin/env python3
import argparse
import json
from difflib import SequenceMatcher


def norm(s):
    return " ".join(str(s or "").strip().lower().split())


def best_match(text, options):
    t = norm(text)
    if not t or not options:
        return {"value": "", "score": 0.0}
    scored = []
    for x in options:
        v = str(x or "").strip()
        if not v:
            continue
        s = SequenceMatcher(None, t, norm(v)).ratio()
        if norm(v) in t or t in norm(v):
            s += 0.2
        scored.append((s, v))
    scored.sort(reverse=True, key=lambda z: z[0])
    best = scored[0] if scored else (0.0, "")
    return {"value": best[1], "score": round(float(best[0]), 3), "top3": [v for _, v in scored[:3]]}


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _to_num(v):
    s = str(v or "").replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _to_pct(v):
    s = str(v or "").strip().lower().replace("%", "")
    if not s:
        return None
    try:
        n = float(s)
        return n / 100.0 if n > 1 else n
    except Exception:
        return None


def _tax_pct_from_name(name):
    s = str(name or "")
    m = best_match(s, [s]).get("value", "")
    txt = str(m or s)
    import re
    pm = re.search(r"(\d+(?:\.\d+)?)\s*%", txt)
    if pm:
        return float(pm.group(1)) / 100.0
    if "non" in txt.lower() or "out of scope" in txt.lower() or "exempt" in txt.lower():
        return 0.0
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parsed", required=True, help="output json from parse_payment_request_xlsx.py")
    ap.add_argument("--bill-rules", required=True, help="bill-rules json response or rules object json")
    args = ap.parse_args()

    parsed = load_json(args.parsed)
    rules_raw = load_json(args.bill_rules)
    rules = rules_raw.get("rules", rules_raw)
    d = (rules.get("qboOptionDictionaries") or {})

    vendors = [x.get("label") or x.get("key") for x in d.get("vendors", []) if isinstance(x, dict)]
    accounts = [x.get("label") or x.get("key") for x in d.get("accounts", []) if isinstance(x, dict)]
    locations = [x.get("label") or x.get("key") for x in d.get("locations", []) if isinstance(x, dict)]
    taxes = [x.get("label") or x.get("key") for x in d.get("taxCodes", []) if isinstance(x, dict)]

    recap = (parsed.get("recap") or {})
    reason = recap.get("reason") or ""

    vendor_source = recap.get("vendor") or reason
    category_source = reason
    location_source = recap.get("location") or recap.get("project_type")

    vendor_match = best_match(vendor_source, vendors)
    vendor_unmatched = (vendor_match.get("score", 0) < 0.55) or (not vendor_match.get("value"))
    category_match = best_match(category_source, accounts)
    location_match = best_match(location_source, locations)
    vat_flag = norm(recap.get("vat_flag") or "")
    if vat_flag in {"vat in", "vatin", "in", "vat in/ex: in"}:
        tax_match = {"value": "VAT 12%", "score": 1.0, "top3": ["VAT 12%"]}
    elif vat_flag in {"vat ex", "vatex", "ex", "vat in/ex: ex"}:
        tax_match = {"value": "Non-Taxable", "score": 1.0, "top3": ["Non-Taxable"]}
    else:
        tax_match = best_match(reason, taxes)

    # Strict WHT rule (upgraded): allow has_wht=true only with explicit rate/amount from Bill Payment 01 extraction.
    wht_rate = _to_pct(recap.get("wht_rate"))
    wht_amount = _to_num(recap.get("wht_amount"))
    explicit_wht = (wht_rate is not None) or (wht_amount is not None)

    line_amounts = [_to_num((x or {}).get("amount")) for x in (recap.get("lines") or [])]
    gross_base = sum(x for x in line_amounts if x is not None)
    tax_pct = _tax_pct_from_name(tax_match.get("value"))
    if tax_pct is None:
        net_base = gross_base
    else:
        net_base = gross_base / (1.0 + tax_pct) if (1.0 + tax_pct) > 0 else gross_base

    expected_wht = (net_base * wht_rate) if (wht_rate is not None) else None
    tolerance = 0.1
    consistent = None
    if expected_wht is not None and wht_amount is not None:
        consistent = abs(expected_wht - wht_amount) <= tolerance

    out = {
        "ok": True,
        "suggestions": {
            "vendor": {
                **vendor_match,
                "unmatched": vendor_unmatched,
                "policy": "if_unmatched_ask_user_then_allow_empty_upload"
            },
            "category": category_match,
            "location": location_match,
            "tax": tax_match,
            "withholding_tax": {
                "has_wht": bool(explicit_wht),
                "rule": "has_wht=true only when Bill Payment 01 provides explicit wht_rate/wht_amount",
                "rate": wht_rate,
                "amount": wht_amount,
                "base": {
                    "gross": round(gross_base, 2),
                    "net": round(net_base, 2),
                    "tax_pct_used": tax_pct
                },
                "expected_amount_from_base": round(expected_wht, 2) if expected_wht is not None else None,
                "consistency_check": {
                    "tolerance": tolerance,
                    "pass": consistent
                },
                "confidence": "high" if explicit_wht else "low"
            }
        },
        "policy": {
            "bill_date_from": "billing_end_date",
            "due_date_from": "excel_due_date",
            "location_fallback": "collector_default_if_empty"
        }
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
