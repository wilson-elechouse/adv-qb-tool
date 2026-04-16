#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import datetime
from pathlib import Path

from canonical_support import (
    canonical_validation_issues,
    format_amount,
    invoke_ai_choice,
    load_bill_rules,
    match_option,
    norm_text,
    parse_number,
    parse_wht_rate_text,
    read_json,
    repo_path,
    sanitize_client_ref,
    strip_combined_ref_text,
    write_json,
)
from field_mapping_support import (
    build_associated_text,
    get_mapping_source_values,
    get_output_field_mapping,
    load_field_mapping_config,
)


def load_category_mapping(path: Path | str | None) -> dict:
    if not path:
        return {}
    fp = Path(path)
    if not fp.exists():
        return {}
    return read_json(fp)


def choose_equals_mapping_option(
    recap: dict,
    options: list[dict[str, str]],
    field_mapping: dict,
    field_name: str,
) -> dict:
    binding = get_output_field_mapping(field_mapping, field_name)
    source_values = get_mapping_source_values(recap, binding)
    if not source_values:
        best = match_option("", options)
    else:
        first = source_values[0]
        best = match_option(first["value"], options)
        best["binding_source_field"] = first["field"]
        best["binding_source_value"] = first["value"]
        best_score = float(best.get("score", 0) or 0)
        for item in source_values[1:]:
            candidate = match_option(item["value"], options)
            score = float(candidate.get("score", 0) or 0)
            if score > best_score:
                best = candidate
                best_score = score
                best["binding_source_field"] = item["field"]
                best["binding_source_value"] = item["value"]
    best["mapping_relation"] = binding.get("relation", "")
    best["mapping_source_fields"] = list(binding.get("sourceFields") or [])
    best["mapping_source_values"] = source_values
    return best


def payment_type_matches(rule_payment_type: str, payment_type: str) -> bool:
    return norm_text(rule_payment_type) == norm_text(payment_type)


def pick_rule_account(recap: dict, mapping: dict, field_mapping: dict) -> tuple[str, str, list[str]]:
    payment_type = recap.get("payment_type", "")
    combined = build_associated_text(recap, get_output_field_mapping(field_mapping, "account_ref_text")).lower()

    hits: list[tuple[str, str]] = []
    for rule in mapping.get("keywordRules") or []:
        when_payment_type = str(rule.get("whenPaymentType") or "").strip()
        if when_payment_type and not payment_type_matches(when_payment_type, payment_type):
            continue
        keywords = [str(item).strip().lower() for item in (rule.get("matchAny") or []) if str(item).strip()]
        if keywords and any(keyword in combined for keyword in keywords):
            hits.append((str(rule.get("account") or ""), f"keyword:{'|'.join(keywords)}"))

    if hits:
        return hits[0][0], "rule_keyword", [reason for _, reason in hits[:3]]

    payment_rules = mapping.get("paymentTypeRules") or {}
    for key, rule in payment_rules.items():
        if payment_type_matches(key, payment_type):
            account = str((rule or {}).get("account") or "")
            if account:
                return account, "rule_payment_type", [key]

    fallback = (mapping.get("fallback") or {}).get(payment_type) or {}
    fallback_account = str(fallback.get("account") or "")
    if fallback_account:
        return fallback_account, "rule_fallback", [payment_type]
    return "", "rule_none", []


def choose_tax(recap: dict, taxes: list[dict[str, str]]) -> dict:
    def option_result(option: dict[str, str], matched_by: str, score: float = 1.0) -> dict:
        ordered = [option.get("label", "")] + [item.get("label", "") for item in taxes if item.get("label") != option.get("label")]
        return {
            "value": option.get("label", ""),
            "ref_id": option.get("ref_id", ""),
            "score": score,
            "alternatives": [label for label in ordered if label][:3],
            "matched_by": matched_by,
        }

    def norm_label_list(options: list[dict[str, str]]) -> list[str]:
        return [norm_text(item.get("label", "")) for item in options]

    def find_by_labels(options: list[dict[str, str]], labels: list[str]) -> dict | None:
        wanted = {norm_text(label) for label in labels if norm_text(label)}
        for option in options:
            if norm_text(option.get("label", "")) in wanted:
                return option
        return None

    def is_wht_tax(option: dict[str, str]) -> bool:
        name = norm_text(option.get("label", ""))
        return "wht" in name or "out of scope" in name

    def positive_tax_options() -> list[dict[str, str]]:
        return [item for item in taxes if tax_pct_from_name(item.get("label", "")) > 0 and not is_wht_tax(item)]

    def zero_tax_options() -> list[dict[str, str]]:
        return [item for item in taxes if tax_pct_from_name(item.get("label", "")) == 0 and not is_wht_tax(item)]

    vat_flag = norm_text(recap.get("vat_flag", ""))
    product = norm_text(recap.get("product", ""))
    reason = norm_text(recap.get("reason", ""))
    if vat_flag in {"vat ex", "vatex", "vat ex."}:
        zero_options = zero_tax_options()
        exact_zero = find_by_labels(zero_options, ["0% Z", "No VAT", "Exempt"])
        if exact_zero:
            return option_result(exact_zero, "inferred_zero_rate")
        if len(zero_options) == 1:
            return option_result(zero_options[0], "inferred_single_zero_rate")
        return match_option("0% Z", taxes)
    if vat_flag in {"vat in", "vatin", "vat in."}:
        taxable_options = positive_tax_options()
        desired_labels = ["12% S"]
        if any(token in f"{product} {reason}" for token in ["consult", "service", "subscription", "digital", "training", "seminar"]):
            desired_labels = ["12% S - Services", "12% S"]
        elif any(token in f"{product} {reason}" for token in ["office supplies", "printing", "purchase", "goods", "booklet", "laptop", "desktop"]):
            desired_labels = ["12% S - Goods", "12% S"]

        exact_taxable = find_by_labels(taxable_options, desired_labels)
        if exact_taxable:
            return option_result(exact_taxable, "inferred_taxable_exact")
        if len(taxable_options) == 1:
            return option_result(taxable_options[0], "inferred_single_taxable_option")
        for desired in desired_labels:
            matched = match_option(desired, taxable_options or taxes)
            if matched.get("score", 0) >= 0.75:
                return matched
        return match_option(desired_labels[0], taxes)
    if any(token in f"{product} {reason}" for token in ["wht", "withholding"]):
        return match_option("WHT-Out of scope", taxes)
    return match_option("0% Z", taxes)


def tax_pct_from_name(name: str) -> float:
    text = norm_text(name)
    if not text:
        return 0.0
    if any(token in text for token in ["0%", "out of scope", "no vat", "exempt"]):
        return 0.0
    import re

    match = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if not match:
        return 0.0
    try:
        return float(match.group(1)) / 100.0
    except Exception:
        return 0.0


def resolve_with_fallback(raw_value: str, options: list[dict[str, str]], *, ai_cmd: str, audit_path: Path, field_name: str, context: dict) -> dict:
    matched = match_option(raw_value, options)
    if matched["score"] >= 0.75:
        matched["source"] = "code_search"
        matched["needs_user_confirmation"] = False
        return matched
    ai_result = invoke_ai_choice(
        ai_cmd=ai_cmd,
        field_name=field_name,
        query_text=raw_value,
        options=options,
        audit_path=audit_path,
        context=context,
    )
    if ai_result.get("ok"):
        return {
            "value": ai_result["value"],
            "ref_id": ai_result.get("ref_id", ""),
            "score": max(float(ai_result.get("score", 0)), float(ai_result.get("ai_confidence", 0))),
            "alternatives": ai_result.get("alternatives", []),
            "source": "ai_fallback",
            "needs_user_confirmation": False,
            "ai_rationale": ai_result.get("ai_rationale", ""),
        }
    matched["source"] = "code_search_unresolved"
    matched["needs_user_confirmation"] = True
    matched["ai_error"] = ai_result.get("error")
    return matched


def choose_account(
    recap: dict,
    account_options: list[dict[str, str]],
    mapping: dict,
    field_mapping: dict,
) -> dict:
    binding = get_output_field_mapping(field_mapping, "account_ref_text")
    if binding.get("relation") == "equals":
        direct = choose_equals_mapping_option(recap, account_options, field_mapping, "account_ref_text")
        direct["source"] = "field_mapping_equals"
        direct["rule_reasons"] = [f"source_field:{item['field']}" for item in direct.get("mapping_source_values", [])]
        direct["needs_user_confirmation"] = (not bool(direct.get("mapping_source_values"))) or direct.get("score", 0) < 0.9
        return direct

    rule_account, rule_source, rule_reasons = pick_rule_account(recap, mapping, field_mapping)
    if rule_account:
        match = match_option(rule_account, account_options)
        if match["value"]:
            match["source"] = rule_source
            match["rule_reasons"] = rule_reasons
            match["mapping_relation"] = binding.get("relation", "")
            match["mapping_source_fields"] = list(binding.get("sourceFields") or [])
            match["mapping_source_values"] = get_mapping_source_values(recap, binding)
            match["needs_user_confirmation"] = match["score"] < 0.9
            return match

    source_text = build_associated_text(recap, binding)
    lexical = match_option(source_text, account_options)
    lexical["source"] = "lexical_fallback"
    lexical["mapping_relation"] = binding.get("relation", "")
    lexical["mapping_source_fields"] = list(binding.get("sourceFields") or [])
    lexical["mapping_source_values"] = get_mapping_source_values(recap, binding)
    lexical["needs_user_confirmation"] = lexical["score"] < 0.75
    return lexical


def build_description(recap: dict) -> str:
    parts = []
    if recap.get("bill_number"):
        parts.append(f"SOA/Bill No: {recap['bill_number']}")
    if recap.get("reason"):
        parts.append(str(recap["reason"]).strip())
    if recap.get("payment_type"):
        parts.append(f"Payment Type: {recap['payment_type']}")
    if recap.get("product"):
        parts.append(f"Product: {recap['product']}")
    if recap.get("which_client"):
        parts.append(f"Client: {recap['which_client']}")
    return " | ".join(parts)[:1000]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parsed", required=True)
    ap.add_argument("--bill-rules", required=True)
    ap.add_argument("--live-rules")
    ap.add_argument("--outDir", required=True)
    ap.add_argument("--chunk-size", type=int, default=10)
    ap.add_argument("--step2")
    ap.add_argument("--rules-source", default="local")
    ap.add_argument("--rules-snapshot", default="")
    ap.add_argument("--rules-hash", default="")
    ap.add_argument("--category-mapping", default=str(repo_path("references", "config", "category-mapping.xnofi.json")))
    ap.add_argument("--field-mapping", default=str(repo_path("references", "config", "field-mapping.xnofi.toml")))
    ap.add_argument("--ai-cmd", default="")
    ap.add_argument("--client-ref-prefix", default="PR-")
    args = ap.parse_args()

    parsed = read_json(args.parsed)
    rules_text = Path(args.bill_rules).read_text(encoding="utf-8")
    rules = load_bill_rules(args.bill_rules)
    live_rules = load_bill_rules(args.live_rules) if args.live_rules and Path(args.live_rules).exists() else {"vendors": [], "accounts": [], "locations": [], "classes": [], "taxes": []}
    mapping = load_category_mapping(args.category_mapping)
    field_mapping = load_field_mapping_config(args.field_mapping)
    effective_hash = args.rules_hash or hashlib.sha256(rules_text.encode("utf-8")).hexdigest()
    records = parsed.get("records") or []
    if not records and parsed.get("recap"):
        records = [{"record_index": 0, "recap": parsed.get("recap", {}), "source_errors": parsed.get("source_errors", [])}]

    out_dir = Path(args.outDir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    vendor_ai_audit_path = out_dir / "ai_audit.vendor.jsonl"
    results = []
    preview_bills = []
    ready_bills = []
    chunk_size = max(1, int(args.chunk_size))
    total_chunks = math.ceil(len(records) / chunk_size) if records else 0

    for i, record in enumerate(records):
        chunk_index = i // chunk_size
        row_in_chunk = i % chunk_size
        record_index = int(record.get("record_index", i))
        recap = record.get("recap") or {}
        source_errors = list(record.get("source_errors") or [])

        vendor_options = rules["vendors"] if rules["vendors"] else live_rules.get("vendors", [])
        location_options = rules["locations"] if rules["locations"] else live_rules.get("locations", [])
        class_options = rules["classes"] if rules["classes"] else live_rules.get("classes", [])
        tax_options = rules["taxes"] if rules["taxes"] else live_rules.get("taxes", [])
        account_options = rules["accounts"] if rules["accounts"] else live_rules.get("accounts", [])

        vendor = resolve_with_fallback(
            str(recap.get("vendor") or ""),
            vendor_options,
            ai_cmd=args.ai_cmd,
            audit_path=vendor_ai_audit_path,
            field_name="vendor_ref_text",
            context={"request_no": recap.get("request_no"), "record_index": record_index},
        )
        location = choose_equals_mapping_option(recap, location_options, field_mapping, "location_ref_text")
        location["source"] = "field_mapping_equals"
        location["needs_user_confirmation"] = bool(location.get("mapping_source_values")) and location["score"] < 0.75

        class_match = choose_equals_mapping_option(recap, class_options, field_mapping, "class_ref_text")
        class_match["source"] = "field_mapping_equals"
        class_match["needs_user_confirmation"] = bool(class_match.get("mapping_source_values")) and class_match["score"] < 0.75

        tax = choose_tax(recap, tax_options)
        tax["source"] = "code_search"
        tax["needs_user_confirmation"] = tax["score"] < 0.75

        account = choose_account(recap, account_options, mapping, field_mapping)

        gross_amount = recap.get("gross_amount")
        if gross_amount is None:
            gross_amount = parse_number((recap.get("lines") or [{}])[0].get("amount"))
        gross_amount_num = float(gross_amount) if gross_amount is not None else 0.0

        wht_rate = parse_wht_rate_text(recap.get("wht_rate"))
        if wht_rate is None:
            try:
                wht_rate = float(recap.get("wht_rate") or 0)
            except Exception:
                wht_rate = 0.0
        wht_amount = parse_number(recap.get("wht_amount"))
        has_wht_input = bool(wht_rate and wht_rate > 0) or bool(wht_amount and wht_amount > 0)

        client_ref = sanitize_client_ref(recap.get("request_no"), prefix=args.client_ref_prefix)
        description = build_description(recap)

        business_line = {
            "account_ref_text": account.get("value") or strip_combined_ref_text(recap.get("product") or ""),
            "account_ref_id": account.get("ref_id", ""),
            "description": description,
            "amount": format_amount(gross_amount_num) if gross_amount is not None else None,
            "class_ref_text": class_match.get("value", ""),
            "class_ref_id": class_match.get("ref_id", ""),
            "tax_ref_text": tax.get("value", ""),
            "tax_ref_id": tax.get("ref_id", ""),
            "meta": {"kind": "business"},
        }

        payload = {
            "vendor_ref_text": vendor.get("value") or strip_combined_ref_text(recap.get("vendor") or ""),
            "vendor_ref_id": vendor.get("ref_id", ""),
            "bill_date": str(recap.get("bill_date") or ""),
            "due_date": str(recap.get("due_date") or ""),
            "location_ref_text": location.get("value", ""),
            "location_ref_id": location.get("ref_id", ""),
            "wht": {"rate": "", "amount": ""},
            "lines": [business_line],
        }
        if not payload["due_date"]:
            payload.pop("due_date")
        if not payload["location_ref_text"]:
            payload.pop("location_ref_text")
            payload.pop("location_ref_id", None)

        if has_wht_input:
            if wht_amount is None:
                business_tax_pct = tax_pct_from_name(tax.get("value", ""))
                base_for_wht = gross_amount_num / (1.0 + business_tax_pct) if (1.0 + business_tax_pct) > 0 else gross_amount_num
                wht_amount = round(abs(base_for_wht) * float(wht_rate or 0), 2)
            if wht_amount and wht_amount > 0:
                payload["wht"] = {
                    "rate": f"{round(float(wht_rate or 0) * 100, 4)}%" if wht_rate else "",
                    "amount": format_amount(wht_amount),
                }
                wht_tax = match_option("WHT-Out of scope", tax_options)
                wht_account = match_option("EWT Payable-BIR", account_options)
                payload["lines"].append(
                    {
                        "account_ref_text": wht_account.get("value") or "EWT Payable-BIR",
                        "account_ref_id": wht_account.get("ref_id", ""),
                        "description": "Withholding tax",
                        "amount": format_amount(-abs(float(wht_amount))),
                        "tax_ref_text": wht_tax.get("value") or tax.get("value", ""),
                        "tax_ref_id": wht_tax.get("ref_id", "") or tax.get("ref_id", ""),
                        "meta": {"kind": "wht"},
                    }
                )

        canonical_bill = {
            "kind": "bill",
            "client_ref": client_ref,
            "memo": "",
            "payload": payload,
        }

        fields = {
            "vendor_ref_text": {
                "value": payload.get("vendor_ref_text", ""),
                "ref_id": payload.get("vendor_ref_id", ""),
                "confidence": vendor.get("score", 0),
                "alternatives": vendor.get("alternatives", []),
                "source": vendor.get("source", ""),
                "needs_user_confirmation": vendor.get("needs_user_confirmation", False),
            },
            "client_ref": {"value": client_ref, "needs_user_confirmation": not bool(client_ref)},
            "bill_date": {"value": payload.get("bill_date", ""), "needs_user_confirmation": not bool(payload.get("bill_date", ""))},
            "due_date": {"value": payload.get("due_date", ""), "needs_user_confirmation": False},
            "location_ref_text": {
                "value": payload.get("location_ref_text", ""),
                "ref_id": payload.get("location_ref_id", ""),
                "confidence": location.get("score", 0),
                "alternatives": location.get("alternatives", []),
                "source": location.get("source", ""),
                "mapping_relation": location.get("mapping_relation", ""),
                "mapping_source_fields": location.get("mapping_source_fields", []),
                "mapping_source_values": location.get("mapping_source_values", []),
                "needs_user_confirmation": location.get("needs_user_confirmation", False),
            },
            "class_ref_text": {
                "value": class_match.get("value", ""),
                "ref_id": class_match.get("ref_id", ""),
                "confidence": class_match.get("score", 0),
                "alternatives": class_match.get("alternatives", []),
                "source": class_match.get("source", ""),
                "mapping_relation": class_match.get("mapping_relation", ""),
                "mapping_source_fields": class_match.get("mapping_source_fields", []),
                "mapping_source_values": class_match.get("mapping_source_values", []),
                "needs_user_confirmation": class_match.get("needs_user_confirmation", False),
            },
            "tax_ref_text": {
                "value": tax.get("value", ""),
                "ref_id": tax.get("ref_id", ""),
                "confidence": tax.get("score", 0),
                "alternatives": tax.get("alternatives", []),
                "source": tax.get("source", ""),
                "needs_user_confirmation": tax.get("needs_user_confirmation", False),
            },
            "account_ref_text": {
                "value": business_line.get("account_ref_text", ""),
                "ref_id": business_line.get("account_ref_id", ""),
                "confidence": account.get("score", 0),
                "alternatives": account.get("alternatives", []),
                "source": account.get("source", ""),
                "needs_user_confirmation": account.get("needs_user_confirmation", False),
                "ai_rationale": account.get("ai_rationale", ""),
                "rule_reasons": account.get("rule_reasons", []),
                "mapping_relation": account.get("mapping_relation", ""),
                "mapping_source_fields": account.get("mapping_source_fields", []),
                "mapping_source_values": account.get("mapping_source_values", []),
                "selected_by": "code",
                "code_candidate": {
                    "value": business_line.get("account_ref_text", ""),
                    "ref_id": business_line.get("account_ref_id", ""),
                    "confidence": account.get("score", 0),
                    "alternatives": account.get("alternatives", []),
                    "source": account.get("source", ""),
                    "needs_user_confirmation": account.get("needs_user_confirmation", False),
                    "rule_reasons": account.get("rule_reasons", []),
                    "mapping_relation": account.get("mapping_relation", ""),
                    "mapping_source_fields": account.get("mapping_source_fields", []),
                    "mapping_source_values": account.get("mapping_source_values", []),
                },
            },
            "category_ref_text": {
                "value": business_line.get("account_ref_text", ""),
                "ref_id": business_line.get("account_ref_id", ""),
                "confidence": account.get("score", 0),
                "alternatives": account.get("alternatives", []),
                "source": account.get("source", ""),
                "needs_user_confirmation": account.get("needs_user_confirmation", False),
                "ai_rationale": account.get("ai_rationale", ""),
                "mapping_relation": account.get("mapping_relation", ""),
                "mapping_source_fields": account.get("mapping_source_fields", []),
                "mapping_source_values": account.get("mapping_source_values", []),
                "selected_by": "code",
            },
            "withholding_tax": {
                "has_wht": has_wht_input,
                "rate": payload["wht"].get("rate", ""),
                "amount": payload["wht"].get("amount", ""),
                "needs_user_confirmation": False,
            },
        }

        unresolved = [
            key
            for key, value in fields.items()
            if isinstance(value, dict) and value.get("needs_user_confirmation")
        ]
        validation_issues = canonical_validation_issues(canonical_bill)
        unresolved += [f"source:{code}" for code in source_errors]
        unresolved += [f"validate:{code}" for code in validation_issues]
        status = "ready"
        if source_errors:
            status = "invalid_source_data"
        elif unresolved or validation_issues:
            status = "needs_user_confirmation"

        match_result = {
            "version": "2.0",
            "ok": True,
            "kind": "bill",
            "status": status,
            "record_index": record_index,
            "source_errors": source_errors,
            "validation_issues": validation_issues,
            "bill_rules_source": args.rules_source or ("live_retry" if args.live_rules else "local"),
            "bill_rules_snapshot": args.rules_snapshot or str(Path(args.bill_rules).resolve()),
            "bill_rules_hash": effective_hash,
            "bill_rules_refreshed_at": datetime.now().isoformat(timespec="seconds"),
            "fields": fields,
            "interaction": {
                "round": 1,
                "unresolved": unresolved,
                "source_errors": source_errors,
                "validation_issues": validation_issues,
            },
            "canonical_bill": canonical_bill,
            "collector_payload": {
                "kind": "bill",
                "client_ref": canonical_bill["client_ref"],
                "memo": canonical_bill["memo"],
                "payload": canonical_bill["payload"],
            },
            "ready_to_upload": status == "ready",
        }

        match_path = out_dir / f"match.chunk{chunk_index + 1}.row{row_in_chunk + 1}.json"
        write_json(match_path, match_result)
        preview_bills.append(canonical_bill)
        if status == "ready":
            ready_bills.append(canonical_bill)
        results.append(
            {
                "chunk_index": chunk_index,
                "row_in_chunk": row_in_chunk,
                "record_index": record_index,
                "match_file": str(match_path.resolve()),
                "status": status,
                "kind": "bill",
                "unresolved": unresolved,
                "source_errors": source_errors,
                "validation_issues": validation_issues,
            }
        )

    preview_path = out_dir / "canonical_bills.preview.json"
    ready_path = out_dir / "canonical_bills.ready.json"
    write_json(preview_path, preview_bills)
    write_json(ready_path, ready_bills)

    summary = {
        "ok": True,
        "total_records": len(records),
        "chunk_size": chunk_size,
        "total_chunks": total_chunks,
        "bill_rules_source": args.rules_source or ("live_retry" if args.live_rules else "local"),
        "bill_rules_snapshot": args.rules_snapshot or str(Path(args.bill_rules).resolve()),
        "bill_rules_hash": effective_hash,
        "field_mapping_file": str(Path(args.field_mapping).resolve()),
        "field_mapping_version": str(field_mapping.get("version") or ""),
        "canonical_preview_file": str(preview_path.resolve()),
        "canonical_ready_file": str(ready_path.resolve()),
        "vendor_ai_audit_file": str(vendor_ai_audit_path.resolve()),
        "results": results,
    }
    summary_path = out_dir / "batch_match_summary.json"
    write_json(summary_path, summary)
    print(
        json.dumps(
            {
                "ok": True,
                "summary": str(summary_path.resolve()),
                "canonical_preview_file": str(preview_path.resolve()),
                "canonical_ready_file": str(ready_path.resolve()),
                "total_records": len(records),
                "total_chunks": total_chunks,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
