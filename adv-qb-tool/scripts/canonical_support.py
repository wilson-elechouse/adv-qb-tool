#!/usr/bin/env python3
from __future__ import annotations

import json
import posixpath
import re
import subprocess
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

XML_NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "pkg": "http://schemas.openxmlformats.org/package/2006/relationships",
}


def repo_path(*parts: str) -> Path:
    return REPO_ROOT.joinpath(*parts)


def script_path(*parts: str) -> Path:
    return SCRIPT_DIR.joinpath(*parts)


def read_json(path: Path | str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path: Path | str, obj: Any) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def append_jsonl(path: Path | str, row: dict[str, Any]) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return out


def norm_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("_", " ").split())


def strip_combined_ref_text(value: Any) -> str:
    text = str(value or "").strip()
    if "·" in text:
        text = text.split("·", 1)[0].strip()
    return text


def clean_token_text(value: Any) -> str:
    text = strip_combined_ref_text(value)
    text = re.sub(r"[^A-Za-z0-9]+", " ", text)
    return norm_text(text)


def parse_number(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"-?\d[\d,]*(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except Exception:
        return None


def format_amount(value: float | int | None) -> float | int | None:
    if value is None:
        return None
    if abs(float(value) - int(float(value))) < 1e-9:
        return int(round(float(value)))
    return round(float(value), 2)


def parse_wht_rate_text(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    num = parse_number(text)
    if num is None or num < 0:
        return None
    if "%" in text or num > 1:
        num = num / 100.0
    return round(num, 6)


def excel_serial_to_date(value: Any) -> str:
    try:
        serial = float(value)
    except Exception:
        return ""
    base = datetime(1899, 12, 30)
    dt = base + timedelta(days=serial)
    return dt.strftime("%Y-%m-%d")


def parse_date_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text:
        return ""
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        return excel_serial_to_date(text)
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%b %d, %Y",
        "%B %d, %Y",
    ):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    if " " in text and len(text.split(" ", 1)[0]) == 10:
        return text.split(" ", 1)[0]
    return ""


def resolve_xlsx_zip_path(target: str, base_dir: str = "xl") -> str:
    raw = str(target or "").replace("\\", "/").strip()
    if not raw:
        raise KeyError("missing xlsx relationship target")
    if raw.startswith("/"):
        resolved = posixpath.normpath(raw.lstrip("/"))
    else:
        resolved = posixpath.normpath(posixpath.join(base_dir, raw))
    if resolved.startswith("../"):
        raise KeyError(f"invalid xlsx relationship target: {target}")
    return resolved.lstrip("/")


def read_first_sheet_rows(xlsx_path: Path | str) -> tuple[str, list[list[str]]]:
    fp = Path(xlsx_path)
    with zipfile.ZipFile(fp) as zf:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("main:si", XML_NS):
                shared_strings.append("".join(t.text or "" for t in si.iterfind(".//main:t", XML_NS)))

        wb = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        relmap = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels.findall("pkg:Relationship", XML_NS)}
        first_sheet = wb.find("main:sheets", XML_NS)[0]
        sheet_name = first_sheet.attrib["name"]
        rel_id = first_sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        sheet_xml = ET.fromstring(zf.read(resolve_xlsx_zip_path(relmap[rel_id])))
        sheet_data = sheet_xml.find("main:sheetData", XML_NS)
        rows_by_index: dict[int, dict[int, str]] = {}
        max_col = 0
        for row in sheet_data.findall("main:row", XML_NS):
            row_index = int(row.attrib.get("r", "0") or 0)
            cell_map: dict[int, str] = {}
            for cell in row.findall("main:c", XML_NS):
                ref = cell.attrib.get("r", "")
                col_letters = re.sub(r"\d+", "", ref)
                col_index = column_index_from_letters(col_letters)
                max_col = max(max_col, col_index)
                cell_map[col_index] = cell_value(cell, shared_strings)
            rows_by_index[row_index] = cell_map

        rows: list[list[str]] = []
        for idx in sorted(rows_by_index):
            row_values = [""] * max_col
            for col_index, value in rows_by_index[idx].items():
                if col_index > 0:
                    row_values[col_index - 1] = value
            rows.append(row_values)
        return sheet_name, rows


def column_index_from_letters(letters: str) -> int:
    value = 0
    for ch in letters.upper():
        if "A" <= ch <= "Z":
            value = value * 26 + (ord(ch) - 64)
    return value


def cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    inline = cell.find("main:is", XML_NS)
    if inline is not None:
        return "".join(t.text or "" for t in inline.iterfind(".//main:t", XML_NS))
    raw = cell.find("main:v", XML_NS)
    if raw is None or raw.text is None:
        return ""
    if cell.attrib.get("t") == "s":
        try:
            return shared_strings[int(raw.text)]
        except Exception:
            return raw.text
    return raw.text


def parse_inline_field_values(text: Any, label: str) -> list[str]:
    pattern = rf"{re.escape(label)}\s*:\s*([^|\n;]+)"
    return [m.strip() for m in re.findall(pattern, str(text or ""), flags=re.IGNORECASE) if m.strip()]


def unique_values(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def sanitize_client_ref(value: Any, prefix: str = "PR-") -> str:
    raw = str(value or "").strip()
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", raw).strip("-")
    if not cleaned:
        return ""
    if prefix and not cleaned.startswith(prefix):
        cleaned = f"{prefix}{cleaned}"
    return cleaned[:64]


def load_bill_rules(path: Path | str) -> dict[str, Any]:
    obj = read_json(path)
    rules = obj.get("rules", obj)
    dictionaries = rules.get("qboOptionDictionaries") or {}
    return {
        "raw": obj,
        "vendors": build_option_list(dictionaries.get("vendors") or [], "qbo_vendor_id", "vendor"),
        "accounts": build_option_list(dictionaries.get("accounts") or [], "qbo_account_id", "account"),
        "locations": build_option_list(dictionaries.get("locations") or [], "qbo_department_id", "department"),
        "classes": build_option_list(dictionaries.get("classes") or [], "qbo_class_id", "class"),
        "taxes": build_option_list(dictionaries.get("taxCodes") or [], "qbo_tax_code_id", "taxcode"),
    }


def build_option_list(items: list[Any], id_field: str, entity: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            label = str(item.get("label") or item.get("key") or "").strip()
            raw_id = str(item.get(id_field) or "").strip()
        else:
            label = str(item or "").strip()
            raw_id = ""
        if not label:
            continue
        ref_id = ""
        if raw_id:
            ref_id = raw_id if ":" in raw_id else f"{entity}:{raw_id}"
        out.append({"label": label, "ref_id": ref_id})
    return out


def match_option(source: Any, options: list[dict[str, str]]) -> dict[str, Any]:
    text = str(source or "").strip()
    if not text:
        return {"value": "", "ref_id": "", "score": 0.0, "alternatives": [], "matched_by": "empty"}
    clean = clean_token_text(text)
    exact = norm_text(strip_combined_ref_text(text))
    scored: list[tuple[float, str, dict[str, str], str]] = []
    source_tokens = set(clean.split())
    for option in options:
        label = option["label"]
        label_norm = norm_text(label)
        label_clean = clean_token_text(label)
        score = 0.0
        matched_by = "none"
        if exact and exact == label_norm:
            score = 1.0
            matched_by = "exact"
        elif clean and clean == label_clean:
            score = 0.98
            matched_by = "clean_exact"
        elif exact and (exact in label_norm or label_norm in exact):
            score = 0.88
            matched_by = "contains"
        elif clean and (clean in label_clean or label_clean in clean):
            score = 0.82
            matched_by = "clean_contains"
        else:
            option_tokens = set(label_clean.split())
            if source_tokens and option_tokens:
                inter = len(source_tokens.intersection(option_tokens))
                union = len(source_tokens.union(option_tokens))
                score = inter / max(union, 1)
                matched_by = "token_jaccard"
        scored.append((round(score, 4), label, option, matched_by))
    scored.sort(key=lambda item: (item[0], len(item[1])), reverse=True)
    top = scored[0] if scored else None
    return {
        "value": top[1] if top else "",
        "ref_id": (top[2].get("ref_id") or "") if top else "",
        "score": float(top[0]) if top else 0.0,
        "alternatives": [item[1] for item in scored[:3]],
        "matched_by": top[3] if top else "none",
    }


def invoke_ai_choice(
    *,
    ai_cmd: str,
    field_name: str,
    query_text: str,
    options: list[dict[str, str]],
    audit_path: Path | str,
    context: dict[str, Any],
) -> dict[str, Any]:
    audit_row: dict[str, Any] = {
        "field_name": field_name,
        "query_text": query_text,
        "options_count": len(options),
        "context": context,
        "used_at": datetime.now().isoformat(timespec="seconds"),
        "ai_cmd": ai_cmd,
    }
    if not ai_cmd.strip():
        audit_row["status"] = "skipped_missing_ai_cmd"
        append_jsonl(audit_path, audit_row)
        return {"ok": False, "error": "ai_cmd_missing"}

    payload = {
        "task": "select_option",
        "input": {
            "field_name": field_name,
            "query_text": query_text,
            "options": [item["label"] for item in options],
            "instruction": (
                "Choose exactly one label from options that best matches query_text. "
                "Return strict JSON only: {\"choice\": \"label\", \"confidence\": 0-1, \"rationale\": \"text\"}. "
                "If no safe match exists, return empty choice with confidence 0."
            ),
        },
        "context": context,
    }
    try:
        proc = subprocess.run(
            ai_cmd,
            input=json.dumps(payload, ensure_ascii=False),
            text=True,
            capture_output=True,
            shell=True,
        )
    except Exception as exc:
        audit_row["status"] = "exec_failed"
        audit_row["error"] = str(exc)
        append_jsonl(audit_path, audit_row)
        return {"ok": False, "error": f"ai_exec_failed:{exc}"}

    audit_row["returncode"] = proc.returncode
    audit_row["stdout"] = (proc.stdout or "").strip()
    audit_row["stderr"] = (proc.stderr or "").strip()
    if proc.returncode != 0:
        audit_row["status"] = "nonzero_exit"
        append_jsonl(audit_path, audit_row)
        return {"ok": False, "error": f"ai_nonzero_exit:{proc.returncode}"}

    parsed = parse_json_fragment(proc.stdout or "")
    choice = str(parsed.get("choice") or "").strip()
    confidence = float(parsed.get("confidence", 0) or 0)
    audit_row["parsed"] = parsed
    audit_row["status"] = "ok" if choice else "empty_choice"
    append_jsonl(audit_path, audit_row)
    if not choice:
        return {"ok": False, "error": "ai_empty_choice"}
    matched = match_option(choice, options)
    if matched["score"] < 0.75:
        return {"ok": False, "error": "ai_choice_not_in_options", "choice": choice, "confidence": confidence}
    matched["ai_choice"] = choice
    matched["ai_confidence"] = confidence
    matched["ai_rationale"] = str(parsed.get("rationale") or "")
    return {"ok": True, **matched}


def parse_json_fragment(text: str) -> dict[str, Any]:
    blob = str(text or "").strip()
    if not blob:
        return {}
    try:
        return json.loads(blob)
    except Exception:
        pass
    match = re.search(r"\{[\s\S]*\}", blob)
    if not match:
        return {}
    try:
        return json.loads(match.group(0))
    except Exception:
        return {}


def canonical_validation_issues(bill: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    kind = str(bill.get("kind") or "")
    if kind and kind != "bill":
        issues.append("kind_must_be_bill")
    client_ref = str(bill.get("client_ref") or "")
    if not client_ref:
        issues.append("client_ref_required")
    elif not re.fullmatch(r"[A-Za-z0-9._-]{3,64}", client_ref):
        issues.append("client_ref_invalid_format")

    memo = str(bill.get("memo") or "")
    if len(memo) > 500:
        issues.append("memo_too_long")

    payload = bill.get("payload") or {}
    vendor = str(payload.get("vendor_ref_text") or "")
    if not vendor:
        issues.append("vendor_ref_text_required")

    bill_date = str(payload.get("bill_date") or "")
    due_date = str(payload.get("due_date") or "")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", bill_date):
        issues.append("bill_date_invalid")
    if due_date and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", due_date):
        issues.append("due_date_invalid")
    if bill_date and due_date and due_date < bill_date:
        issues.append("due_date_before_bill_date")

    lines = payload.get("lines") or []
    if not isinstance(lines, list) or not lines:
        issues.append("lines_required")
        return issues
    if len(lines) > 200:
        issues.append("lines_too_many")

    total = 0.0
    wht_lines = 0
    wht_input = payload.get("wht") or {}
    has_wht_input = str(wht_input.get("rate") or "").strip() != "" or str(wht_input.get("amount") or "").strip() != ""
    for idx, line in enumerate(lines, start=1):
        if "location_ref_text" in line and str(line.get("location_ref_text") or "").strip():
            issues.append(f"line{idx}.location_not_allowed")
        account = str(line.get("account_ref_text") or "")
        if not account:
            issues.append(f"line{idx}.account_required")
        amount = line.get("amount")
        if not isinstance(amount, (int, float)):
            issues.append(f"line{idx}.amount_required")
            continue
        total += float(amount)
        meta = line.get("meta") or {}
        kind_value = str(meta.get("kind") or "business")
        if kind_value == "wht":
            wht_lines += 1
            if float(amount) >= 0:
                issues.append(f"line{idx}.wht_amount_must_be_negative")
        else:
            if float(amount) <= 0:
                issues.append(f"line{idx}.business_amount_must_be_positive")
    if total <= 0:
        issues.append("sum_lines_amount_must_be_positive")
    if has_wht_input and wht_lines != 1:
        issues.append("wht_input_requires_exactly_one_wht_line")
    if not has_wht_input and wht_lines != 0:
        issues.append("wht_line_not_allowed_without_wht_input")
    return issues
