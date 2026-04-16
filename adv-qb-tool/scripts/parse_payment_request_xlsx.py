#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from canonical_support import (
    norm_text,
    parse_date_text,
    parse_inline_field_values,
    parse_number,
    parse_wht_rate_text,
    read_first_sheet_rows,
    unique_values,
    write_json,
)
from field_mapping_support import load_field_mapping_config, merge_header_aliases


def detect_map(headers: list[str], header_aliases: dict[str, list[str]]) -> dict[str, int]:
    idx: dict[str, int] = {}
    normalized = [norm_text(h) for h in headers]
    for key, aliases in header_aliases.items():
        for index, header in enumerate(normalized):
            if any(alias in header for alias in aliases):
                idx[key] = index
                break
    return idx


def make_getter(header_map: dict[str, int]):
    def get(row: list[str], key: str) -> str:
        idx = header_map.get(key)
        if idx is None or idx >= len(row):
            return ""
        value = row[idx]
        return "" if value is None else str(value).strip()

    return get


def normalize_scalar(value: str) -> str:
    return " ".join(str(value or "").split())


def extract_unique(text: str, label: str) -> tuple[str, list[str]]:
    values = unique_values([normalize_scalar(v) for v in parse_inline_field_values(text, label)])
    if not values:
        return "", []
    return values[0], values


def parse_record(record_index: int, row_number: int, row: list[str], get, status_detected: bool) -> dict:
    pd01_text = get(row, "payment_details_01")
    pd02_text = get(row, "payment_details_02")
    payment_type_header = normalize_scalar(get(row, "payment_type"))

    supplier_value, supplier_values = extract_unique(pd02_text, "Which Supplier")
    client_value, client_values = extract_unique(pd02_text, "Which Client")
    product_value, product_values = extract_unique(pd02_text, "Product")
    payment_type_inline_value, payment_type_values = extract_unique(pd01_text, "Payment Type")
    payment_type_value = payment_type_header or payment_type_inline_value
    gross_amount_text, gross_amount_values = extract_unique(pd01_text, "Payables Amount-Gross")
    inline_due_text, inline_due_values = extract_unique(pd01_text, "Due date")
    inline_wht_rate_text, inline_wht_rate_values = extract_unique(pd01_text, "2307 Rate")
    inline_wht_amount_text, inline_wht_amount_values = extract_unique(pd01_text, "2307 Amount")
    inline_vat_amount_text, inline_vat_amount_values = extract_unique(pd01_text, "VAT Amount")
    inline_net_amount_text, inline_net_amount_values = extract_unique(pd01_text, "Net Amount-PHP")

    source_errors: list[str] = []
    if len(supplier_values) > 1:
        source_errors.append("payment_details_02_multiple_suppliers")
    if len(client_values) > 1:
        source_errors.append("payment_details_02_multiple_clients")
    if len(product_values) > 1:
        source_errors.append("payment_details_02_multiple_products")
    if len(payment_type_values) > 1:
        source_errors.append("payment_details_01_multiple_payment_types")
    if len(gross_amount_values) > 1:
        source_errors.append("payment_details_01_multiple_gross_amounts")
    if len(inline_due_values) > 1:
        source_errors.append("payment_details_01_multiple_due_dates")
    if len(inline_wht_rate_values) > 1:
        source_errors.append("payment_details_01_multiple_wht_rates")
    if len(inline_wht_amount_values) > 1:
        source_errors.append("payment_details_01_multiple_wht_amounts")
    if len(inline_vat_amount_values) > 1:
        source_errors.append("payment_details_01_multiple_vat_amounts")
    if len(inline_net_amount_values) > 1:
        source_errors.append("payment_details_01_multiple_net_amounts")

    billing_start_date = parse_date_text(get(row, "billing_start_date"))
    billing_end_date = parse_date_text(get(row, "billing_end_date"))
    due_date_column = parse_date_text(get(row, "due_date"))
    due_date_inline = parse_date_text(inline_due_text)
    due_date = due_date_column or due_date_inline
    bill_date = billing_start_date

    vendor_raw = supplier_value or get(row, "vendor")
    amount_header = parse_number(get(row, "amount"))
    gross_amount = parse_number(gross_amount_text) if gross_amount_text else amount_header
    if gross_amount is None:
        source_errors.append("gross_amount_missing")

    if not vendor_raw:
        source_errors.append("vendor_missing")
    if not bill_date:
        source_errors.append("billing_start_date_missing")
    if not get(row, "request_no"):
        source_errors.append("request_no_missing")

    wht_rate = parse_wht_rate_text(get(row, "wht_rate"))
    if wht_rate is None:
        wht_rate = parse_wht_rate_text(inline_wht_rate_text)
    if wht_rate is None:
        wht_rate = 0.0

    wht_amount = parse_number(get(row, "wht_amount"))
    if wht_amount is None:
        wht_amount = parse_number(inline_wht_amount_text)
    vat_amount = parse_number(inline_vat_amount_text)
    net_amount = parse_number(inline_net_amount_text)

    if due_date and bill_date and due_date < bill_date:
        source_errors.append("due_date_before_bill_date")

    if status_detected and norm_text(get(row, "status")) != "approved":
        source_errors.append("status_not_approved")

    recap = {
        "request_no": get(row, "request_no"),
        "client_ref_candidate": get(row, "request_no"),
        "bill_number": get(row, "bill_no"),
        "vendor": vendor_raw,
        "vendor_source": "payment_details_02.which_supplier" if supplier_value else ("header.vendor" if get(row, "vendor") else "missing"),
        "which_client": client_value,
        "product": product_value,
        "payment_type": payment_type_value,
        "payment_type_source": "header.payment_type" if payment_type_header else ("payment_details_01.payment_type" if payment_type_inline_value else "missing"),
        "bill_date": bill_date,
        "due_date": due_date,
        "location": get(row, "location"),
        "belongs_to": get(row, "belongs_to"),
        "project_type": get(row, "project_type"),
        "reason": get(row, "reason"),
        "payment_detail_01_text": pd01_text,
        "payment_detail_02_text": pd02_text,
        "billing_start_date": billing_start_date,
        "billing_end_date": billing_end_date,
        "gross_amount": gross_amount,
        "net_amount": net_amount,
        "vat_amount": vat_amount,
        "wht_rate": wht_rate,
        "wht_amount": wht_amount,
        "vat_flag": get(row, "vat_flag"),
        "lines": [{"amount": gross_amount}] if gross_amount is not None else [],
    }
    missing_required = [key for key, ok in {
        "request_no": bool(recap["request_no"]),
        "vendor": bool(recap["vendor"]),
        "bill_date": bool(recap["bill_date"]),
        "gross_amount": recap["gross_amount"] is not None,
    }.items() if not ok]

    return {
        "record_index": record_index,
        "row_number": row_number,
        "status": get(row, "status"),
        "source_errors": source_errors,
        "missing_required": missing_required,
        "source": {
            "request_no": get(row, "request_no"),
            "bill_no": get(row, "bill_no"),
            "status": get(row, "status"),
            "billing_start_date": get(row, "billing_start_date"),
            "billing_end_date": get(row, "billing_end_date"),
            "due_date": get(row, "due_date"),
            "location": get(row, "location"),
            "belongs_to": get(row, "belongs_to"),
            "vat_flag": get(row, "vat_flag"),
            "payment_details_01": pd01_text,
            "payment_details_02": pd02_text,
            "reason": get(row, "reason"),
            "payment_type": get(row, "payment_type"),
        },
        "recap": recap,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--field-mapping")
    args = ap.parse_args()

    source_file = Path(args.file).resolve()
    out_path = Path(args.out).resolve()
    field_mapping_path = Path(args.field_mapping).resolve() if args.field_mapping else None
    field_mapping = load_field_mapping_config(field_mapping_path)
    header_aliases = merge_header_aliases(field_mapping)
    sheet_name, rows = read_first_sheet_rows(source_file)
    if not rows:
        out = {"ok": False, "error": "empty_sheet", "file": str(source_file)}
        write_json(out_path, out)
        print(json.dumps(out, ensure_ascii=False))
        return

    headers = [str(cell or "").strip() for cell in rows[0]]
    header_map = detect_map(headers, header_aliases)
    get = make_getter(header_map)

    data_rows = [row for row in rows[1:] if any(str(cell or "").strip() for cell in row)]
    if not data_rows:
        out = {"ok": False, "error": "no_data_rows", "file": str(source_file)}
        write_json(out_path, out)
        print(json.dumps(out, ensure_ascii=False))
        return

    status_detected = "status" in header_map
    approved_rows: list[tuple[int, list[str]]] = []
    non_approved_count = 0
    for row_number, row in enumerate(data_rows, start=2):
        if status_detected:
            if norm_text(get(row, "status")) == "approved":
                approved_rows.append((row_number, row))
            else:
                non_approved_count += 1
        else:
            approved_rows.append((row_number, row))

    if not approved_rows:
        out = {
            "ok": False,
            "error": "no_approved_rows",
            "file": str(source_file),
            "rows": {
                "total": len(data_rows),
                "approved": 0,
                "ignored_non_approved": non_approved_count,
                "status_column_detected": status_detected,
            },
        }
        write_json(out_path, out)
        print(json.dumps(out, ensure_ascii=False))
        return

    records = [
        parse_record(record_index=index, row_number=row_number, row=row, get=get, status_detected=status_detected)
        for index, (row_number, row) in enumerate(approved_rows)
    ]

    error_summary: dict[str, int] = {}
    missing_required_total = 0
    for record in records:
        for code in record["source_errors"]:
            error_summary[code] = error_summary.get(code, 0) + 1
        missing_required_total += len(record["missing_required"])

    first_record = records[0]
    out = {
        "ok": True,
        "step": "parse_identify",
        "file": str(source_file),
        "sheet": sheet_name,
        "kind": "bill",
        "kind_confidence": "high",
        "header_map": header_map,
        "field_mapping_file": str(field_mapping_path) if field_mapping_path else "",
        "field_mapping_version": str(field_mapping.get("version") or ""),
        "rows": {
            "total": len(data_rows),
            "approved": len(approved_rows),
            "ignored_non_approved": non_approved_count,
            "status_column_detected": status_detected,
            "filter_mode": "strict_approved_only" if status_detected else "no_status_column_all_rows_used",
        },
        "records": records,
        "recap": first_record["recap"],
        "missing_required": first_record["missing_required"],
        "parse_error_count": sum(len(record["source_errors"]) for record in records),
        "parse_error_summary": error_summary,
        "records_with_source_errors": sum(1 for record in records if record["source_errors"]),
        "records_with_missing_required": sum(1 for record in records if record["missing_required"]),
        "missing_required_total": missing_required_total,
        "parse_hints": {
            "vendor_rule": "row.payment_details_02 -> Which Supplier",
            "amount_rule": "row.payment_details_01 -> Payables Amount-Gross",
            "due_date_rule": "row.due_date or payment_details_01 -> Due date",
            "bill_date_rule": "bill_date <- billing_start_date",
            "malformed_multi_segment_policy": "record_source_error",
        },
    }

    write_json(out_path, out)
    print(json.dumps({"ok": True, "out": str(out_path), "records": len(records), "parse_error_count": out["parse_error_count"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
