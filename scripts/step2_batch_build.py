#!/usr/bin/env python3
import argparse, json, math, hashlib
from datetime import datetime
from pathlib import Path


def norm(s):
    return " ".join(str(s or "").strip().lower().split())


def match_one(source, options):
    s = norm(source)
    if not s or not options:
        return {"value": "", "score": 0.0, "top3": []}
    scored = []
    sset = set(s.split())
    for o in options:
        v = str(o or "").strip()
        if not v:
            continue
        nv = norm(v)
        if nv == s:
            score = 1.0
        elif nv in s or s in nv:
            score = 0.75
        else:
            vset = set(nv.split())
            inter = len(sset.intersection(vset))
            score = inter / max(len(sset), len(vset), 1)
        scored.append((score, v))
    scored.sort(key=lambda x: x[0], reverse=True)
    return {
        "value": scored[0][1] if scored else "",
        "score": round(scored[0][0], 3) if scored else 0.0,
        "top3": [x[1] for x in scored[:3]],
    }


def dict_values(d, key):
    out = []
    for x in (d.get(key) or []):
        if isinstance(x, dict):
            v = x.get("label") or x.get("key")
        else:
            v = x
        if v:
            out.append(str(v))
    return out


def dict_ref_ids(d, key, id_field, entity):
    out = {}
    for x in (d.get(key) or []):
        if not isinstance(x, dict):
            continue
        label = str(x.get("label") or x.get("key") or "").strip()
        qbo_id = str(x.get(id_field) or "").strip()
        if not label or not qbo_id or qbo_id.startswith("__"):
            continue
        out[norm(label)] = f"{entity}:{qbo_id}"
    return out


def ref_id_for(ref_map, value):
    return ref_map.get(norm(value), "")


def parse_rules(obj):
    rules = obj.get("rules", obj)
    q = rules.get("qboOptionDictionaries") or {}
    return {
        "vendors": dict_values(q, "vendors"),
        "vendor_ref_ids": dict_ref_ids(q, "vendors", "qbo_vendor_id", "vendor"),
        "accounts": dict_values(q, "accounts"),
        "account_ref_ids": dict_ref_ids(q, "accounts", "qbo_account_id", "account"),
        "locations": dict_values(q, "locations"),
        "location_ref_ids": dict_ref_ids(q, "locations", "qbo_department_id", "department"),
        "taxes": dict_values(q, "taxCodes"),
        "tax_ref_ids": dict_ref_ids(q, "taxCodes", "qbo_tax_code_id", "taxcode"),
        "classes": dict_values(q, "classes"),
        "class_ref_ids": dict_ref_ids(q, "classes", "qbo_class_id", "class"),
    }


def pick_tax(vat_flag, taxes, reason=""):
    vf = norm(vat_flag)
    if vf in {"vat in", "vatin", "in", "vat in/ex: in"}:
        m = match_one("12% S - Goods", taxes)
        if m.get("score", 0) >= 0.55:
            return m
    if vf in {"vat ex", "vatex", "ex", "vat in/ex: ex"}:
        m = match_one("0% Z", taxes)
        if m.get("score", 0) >= 0.55:
            return m
    return match_one(reason, taxes)


def tax_pct_from_name(name: str):
    s = norm(name)
    if not s:
        return 0.0
    if "out of scope" in s or "no vat" in s or "non" in s or "exempt" in s or "0%" in s:
        return 0.0
    import re
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", s)
    if m:
        try:
            return float(m.group(1)) / 100.0
        except Exception:
            return 0.0
    return 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parsed", required=True)
    ap.add_argument("--bill-rules", required=True)
    ap.add_argument("--live-rules", help="optional live bill-rules snapshot for retry")
    ap.add_argument("--outDir", required=True)
    ap.add_argument("--chunk-size", type=int, default=10)
    ap.add_argument("--step2", help="step2_ai_judge.json")
    ap.add_argument("--rules-source", default="local", help="local|live_retry|manual_refresh")
    ap.add_argument("--rules-snapshot", default="", help="effective rules snapshot path")
    ap.add_argument("--rules-hash", default="", help="effective rules hash")
    args = ap.parse_args()

    parsed = json.loads(Path(args.parsed).read_text(encoding="utf-8"))
    bill_rules_raw_text = Path(args.bill_rules).read_text(encoding="utf-8")
    rules = parse_rules(json.loads(bill_rules_raw_text))
    live_rules = parse_rules(json.loads(Path(args.live_rules).read_text(encoding="utf-8"))) if args.live_rules and Path(args.live_rules).exists() else {"vendors": [], "accounts": [], "locations": [], "taxes": []}
    effective_hash = args.rules_hash or hashlib.sha256(bill_rules_raw_text.encode("utf-8")).hexdigest()
    records = parsed.get("records") or []
    if not records and parsed.get("recap"):
        records = [{"record_index": 0, "recap": parsed.get("recap", {})}]

    ai_by_idx = {}
    if args.step2 and Path(args.step2).exists():
        s2 = json.loads(Path(args.step2).read_text(encoding="utf-8"))
        for r in s2.get("records", []):
            ai_by_idx[int(r.get("record_index", 0))] = r.get("category_ai") or {}

    out_dir = Path(args.outDir)
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    chunk_size = max(1, int(args.chunk_size))
    total_chunks = math.ceil(len(records) / chunk_size) if records else 0

    for i, rec in enumerate(records):
        cidx = i // chunk_size
        ridx = i % chunk_size
        recap = rec.get("recap", {})

        vendor_src = recap.get("vendor", "")
        vendor = match_one(vendor_src, rules["vendors"])
        if vendor.get("score", 0) < 0.55 and live_rules["vendors"]:
            vendor = match_one(vendor_src, live_rules["vendors"])

        location_src = recap.get("location") or recap.get("project_type") or ""
        location = match_one(location_src, rules["locations"])
        if location_src and location.get("score", 0) < 0.55 and live_rules["locations"]:
            location = match_one(location_src, live_rules["locations"])

        class_src = recap.get("belongs_to", "")
        class_match = match_one(class_src, rules["classes"])
        if class_src and class_match.get("score", 0) < 0.55 and live_rules["classes"]:
            class_match = match_one(class_src, live_rules["classes"])

        tax = pick_tax(recap.get("vat_flag", ""), rules["taxes"], recap.get("reason", ""))
        if tax.get("score", 0) < 0.55 and live_rules["taxes"]:
            tax = pick_tax(recap.get("vat_flag", ""), live_rules["taxes"], recap.get("reason", ""))

        ai_cat = ai_by_idx.get(int(rec.get("record_index", i)), {})
        if ai_cat.get("category_ref_text"):
            category = {
                "value": ai_cat.get("category_ref_text", ""),
                "score": float(ai_cat.get("confidence", 0) or 0),
                "top3": ai_cat.get("top3") or [],
                "rationale": ai_cat.get("rationale", "")
            }
        else:
            category_src = " | ".join([recap.get("payment_detail_01_text", ""), recap.get("payment_detail_02_text", ""), recap.get("reason", "")])
            category = match_one(category_src, rules["accounts"])
            category["rationale"] = "dictionary lexical match"

        wht_rate_val = recap.get("wht_rate")
        try:
            wht_rate_num = float(wht_rate_val) if wht_rate_val not in (None, "") else 0.0
        except Exception:
            wht_rate_num = 0.0
        has_wht = wht_rate_num > 0
        wht = {
            "has_wht": has_wht,
            "rate": wht_rate_num if has_wht else 0.0,
            "amount": recap.get("wht_amount") or None,
            "source": "rule+program(rate_only_decider)",
            "needs_user_confirmation": False,
        }

        fields = {
            "vendor_ref_text": {"value": vendor["value"], "ref_id": ref_id_for(rules["vendor_ref_ids"], vendor["value"]) or ref_id_for(live_rules.get("vendor_ref_ids", {}), vendor["value"]), "confidence": vendor["score"], "alternatives": vendor["top3"], "needs_user_confirmation": vendor["score"] < 0.55},
            "client_ref": {"value": recap.get("request_no", ""), "needs_user_confirmation": False},
            "bill_no": {"value": recap.get("bill_number", ""), "needs_user_confirmation": False},
            "bill_date": {"value": recap.get("bill_date", ""), "needs_user_confirmation": False},
            "due_date": {"value": recap.get("due_date", ""), "needs_user_confirmation": not bool(recap.get("due_date"))},
            "category_ref_text": {"value": category.get("value", ""), "ref_id": ref_id_for(rules["account_ref_ids"], category.get("value", "")) or ref_id_for(live_rules.get("account_ref_ids", {}), category.get("value", "")), "confidence": category.get("score", 0), "alternatives": category.get("top3", []), "ai_rationale": category.get("rationale", ""), "needs_user_confirmation": not bool(category.get("value"))},
            "location_ref_text": {"value": location["value"] if location["score"] >= 0.55 else "", "ref_id": ref_id_for(rules["location_ref_ids"], location.get("value", "")) or ref_id_for(live_rules.get("location_ref_ids", {}), location.get("value", "")), "confidence": location["score"], "alternatives": location["top3"], "fallback": "collector_default_if_empty", "use_collector_default_when_empty": True if location["score"] < 0.55 else False, "needs_user_confirmation": False},
            "tax_code_ref_text": {"value": tax["value"], "ref_id": ref_id_for(rules["tax_ref_ids"], tax.get("value", "")) or ref_id_for(live_rules.get("tax_ref_ids", {}), tax.get("value", "")), "confidence": tax.get("score", 0), "alternatives": tax.get("top3", []), "needs_user_confirmation": not bool(tax.get("value"))},
            "class_ref_text": {"value": class_match["value"] if class_match.get("score", 0) >= 0.55 else "", "ref_id": ref_id_for(rules["class_ref_ids"], class_match.get("value", "")) or ref_id_for(live_rules.get("class_ref_ids", {}), class_match.get("value", "")), "confidence": class_match.get("score", 0), "alternatives": class_match.get("top3", []), "needs_user_confirmation": False},
            "withholding_tax": wht,
        }

        unresolved = [k for k, v in fields.items() if isinstance(v, dict) and v.get("needs_user_confirmation")]
        status = "needs_user_confirmation" if unresolved else "ready"

        base_amount = (recap.get("lines") or [{}])[0].get("amount", 10000)
        try:
            base_amount_num = float(base_amount)
        except Exception:
            base_amount_num = 10000.0

        business_tax_name = fields["tax_code_ref_text"]["value"]
        business_tax_pct = tax_pct_from_name(business_tax_name)
        net_base_total = base_amount_num / (1.0 + business_tax_pct) if (1.0 + business_tax_pct) > 0 else base_amount_num

        lines = [{
            "amount": base_amount_num,
            "account_ref_text": fields["category_ref_text"]["value"],
            "account_ref_id": fields["category_ref_text"].get("ref_id", ""),
            "category_ref_text": fields["category_ref_text"]["value"],
            "tax_ref_text": business_tax_name,
            "tax_ref_id": fields["tax_code_ref_text"].get("ref_id", ""),
            "class_ref_text": fields["class_ref_text"]["value"],
            "class_ref_id": fields["class_ref_text"].get("ref_id", ""),
            "description": "; ".join([
                f"Feishu: {recap.get('bill_number', '')}",
                f"Billing Date: {recap.get('billing_end_date') or recap.get('bill_date', '')}",
                f"Period Covered: {recap.get('billing_end_date') or recap.get('bill_date', '')}",
                f"Business Unit: {recap.get('belongs_to', '')}",
                f"Reason: {recap.get('reason', '')}",
            ]),
        }]

        wht_amount_num = None
        if has_wht:
            wht_tax = match_one("WHT-Out of scope", rules["taxes"])
            if wht_tax.get("score", 0) < 0.55 and live_rules["taxes"]:
                wht_tax = match_one("WHT-Out of scope", live_rules["taxes"])
            wht_amount = recap.get("wht_amount")
            try:
                wht_amount_num = abs(float(wht_amount)) if wht_amount not in (None, "") else round(abs(net_base_total) * wht_rate_num, 2)
            except Exception:
                wht_amount_num = round(abs(net_base_total) * wht_rate_num, 2)
            if wht_amount_num > 0:
                wht["amount"] = round(float(wht_amount_num), 2)
                lines.append({
                    "amount": -wht_amount_num,
                    "account_ref_text": "EWT Payable-BIR",
                    "account_ref_id": ref_id_for(rules["account_ref_ids"], "EWT Payable-BIR") or ref_id_for(live_rules.get("account_ref_ids", {}), "EWT Payable-BIR"),
                    "category_ref_text": "EWT Payable-BIR",
                    "tax_ref_text": wht_tax.get("value") or fields["tax_code_ref_text"]["value"],
                    "tax_ref_id": ref_id_for(rules["tax_ref_ids"], wht_tax.get("value") or fields["tax_code_ref_text"]["value"]) or ref_id_for(live_rules.get("tax_ref_ids", {}), wht_tax.get("value") or fields["tax_code_ref_text"]["value"]),
                    "class_ref_text": "",
                    "class_ref_id": "",
                    "description": "Withholding tax",
                    "meta": {"kind": "wht"},
                })

        m = {
            "version": "1.0",
            "ok": True,
            "kind": parsed.get("kind", "bill"),
            "status": status,
            "bill_rules_source": args.rules_source if args.rules_source else ("live_retry" if args.live_rules else "local"),
            "bill_rules_snapshot": args.rules_snapshot or str(Path(args.bill_rules).resolve()),
            "bill_rules_hash": effective_hash,
            "bill_rules_refreshed_at": datetime.now().isoformat(timespec="seconds"),
            "fields": fields,
            "interaction": {"round": 1, "unresolved": unresolved},
            "collector_payload": {
                "kind": parsed.get("kind", "bill"),
                "draft": {
                    "client_ref": fields["client_ref"]["value"],
                    "bill_no": fields["bill_no"]["value"],
                    "bill_date": fields["bill_date"]["value"],
                    "due_date": fields["due_date"]["value"],
                    "vendor_ref_text": fields["vendor_ref_text"]["value"],
                    "vendor_ref_id": fields["vendor_ref_text"].get("ref_id", ""),
                    "location_ref_text": fields["location_ref_text"]["value"] or ("__USE_DEFAULT_LOCATION__" if fields["location_ref_text"].get("use_collector_default_when_empty") else ""),
                    "location_ref_id": fields["location_ref_text"].get("ref_id", "") if fields["location_ref_text"]["value"] else "",
                    "lines": lines,
                    "wht": {
                        "rate": wht.get("rate") if has_wht else "",
                        "amount": wht.get("amount") if wht.get("amount") not in (None, "") else "",
                    },
                    "withholding_tax": {
                        **wht,
                        "base": {
                            "gross": round(base_amount_num, 2),
                            "net": round(net_base_total, 2),
                            "tax_pct_used": business_tax_pct,
                        },
                    },
                }
            },
            "ready_to_upload": status in ["ready", "ready_with_blanks"]
        }

        mpath = out_dir / f"match.chunk{cidx+1}.row{ridx+1}.json"
        mpath.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")
        results.append({
            "chunk_index": cidx,
            "row_in_chunk": ridx,
            "record_index": rec.get("record_index", i),
            "match_file": str(mpath.resolve()),
            "status": m.get("status", status),
            "kind": m["kind"],
            "unresolved": m.get("interaction", {}).get("unresolved", unresolved),
        })

    summary = {
        "ok": True,
        "total_records": len(records),
        "chunk_size": chunk_size,
        "total_chunks": total_chunks,
        "bill_rules_source": args.rules_source if args.rules_source else ("live_retry" if args.live_rules else "local"),
        "bill_rules_snapshot": args.rules_snapshot or str(Path(args.bill_rules).resolve()),
        "bill_rules_hash": effective_hash,
        "results": results,
    }
    s_path = out_dir / "batch_match_summary.json"
    s_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "summary": str(s_path.resolve()), "total_records": len(records), "total_chunks": total_chunks}, ensure_ascii=False))


if __name__ == "__main__":
    main()
