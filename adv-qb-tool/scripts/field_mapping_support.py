#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None

DEFAULT_HEADER_ALIASES = {
    "vendor": ["vendor", "vendor name", "supplier", "payee"],
    "bill_no": ["billing/soa no.", "billing soa no", "soa no", "billing no", "bill no", "feishu"],
    "request_no": ["request no.", "request no", "request number", "req no", "req#"],
    "billing_end_date": ["billing end date", "billing date", "end date"],
    "billing_start_date": ["billing start date", "start date"],
    "due_date": ["due date", "payment due date", "due"],
    "status": ["status", "approval status", "request status"],
    "location": ["location", "site", "branch"],
    "belongs_to": ["belongs to", "business unit", "bu", "class"],
    "project_type": ["project type", "project", "location mapping"],
    "reason": ["reason", "reasons for payment", "payment reason", "description reason"],
    "payment_type": ["payment type", "pay type"],
    "payment_details_01": ["payment detail 01", "payment details 01", "payment detail01", "payment details01"],
    "payment_details_02": ["payment detail 02", "payment details 02", "payment detail02", "payment details02"],
    "category": ["category", "account", "account name", "expense account"],
    "amount": ["amount", "line amount", "gross amount", "total amount"],
    "wht_rate": ["wht rate", "withholding tax rate", "withholding rate", "ewt rate"],
    "wht_amount": ["wht amount", "withholding tax amount", "ewt amount"],
    "vat_flag": ["vat in/ex", "vat in ex", "vat in", "vat ex", "vat type", "vat"],
}

DEFAULT_OUTPUT_FIELD_MAPPINGS = {
    "vendor_ref_text": {
        "relation": "equals",
        "sourceFields": ["vendor"],
    },
    "location_ref_text": {
        "relation": "equals",
        "sourceFields": ["location", "project_type"],
    },
    "class_ref_text": {
        "relation": "equals",
        "sourceFields": ["belongs_to"],
    },
    "account_ref_text": {
        "relation": "associated",
        "sourceFields": ["payment_type", "product", "reason", "payment_detail_02_text", "vendor"],
    },
    "category_ref_text": {
        "relation": "associated",
        "sourceFields": ["payment_type", "product", "reason", "payment_detail_02_text", "vendor"],
    },
}

TARGET_FIELD_ALIASES = {
    "vendor": "vendor_ref_text",
    "location": "location_ref_text",
    "class": "class_ref_text",
    "account": "account_ref_text",
    "category": "category_ref_text",
}


def _normalize_string_list(raw_values: Any) -> list[str]:
    values = raw_values if isinstance(raw_values, list) else [raw_values]
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        normalized = text.lower()
        if not text or normalized in seen:
            continue
        seen.add(normalized)
        out.append(text)
    return out


def _normalize_header_aliases_config(raw: Any) -> dict[str, list[str]]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, list[str]] = {}
    for key, raw_values in raw.items():
        values = _normalize_string_list(raw_values)
        if values:
            out[str(key)] = values
    return out


def _normalize_target_field_name(name: Any) -> str:
    key = str(name or "").strip()
    if not key:
        return ""
    return TARGET_FIELD_ALIASES.get(key, key)


def _normalize_output_mappings_config(raw: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for key, raw_mapping in raw.items():
        if not isinstance(raw_mapping, dict):
            continue
        target = _normalize_target_field_name(raw_mapping.get("target") or key)
        if not target:
            continue
        normalized = {
            "relation": normalize_relation(raw_mapping.get("relation") or raw_mapping.get("match")),
            "sourceFields": _normalize_string_list(
                raw_mapping.get("sourceFields")
                or raw_mapping.get("source_fields")
                or raw_mapping.get("from")
                or []
            ),
        }
        for extra_key, extra_value in raw_mapping.items():
            if extra_key in {"target", "relation", "match", "sourceFields", "source_fields", "from"}:
                continue
            normalized[extra_key] = copy.deepcopy(extra_value)
        out[target] = normalized
    return out


def normalize_field_mapping_config(config: dict[str, Any] | None) -> dict[str, Any]:
    raw = config if isinstance(config, dict) else {}
    normalized = copy.deepcopy(raw)
    normalized["headerAliases"] = _normalize_header_aliases_config(
        raw.get("headerAliases")
        or raw.get("header_aliases")
        or raw.get("excel_columns")
        or {}
    )
    normalized["outputFieldMappings"] = _normalize_output_mappings_config(
        raw.get("outputFieldMappings")
        or raw.get("output_fields")
        or raw.get("outputs")
        or {}
    )
    return normalized


def load_field_mapping_config(path: Path | str | None) -> dict[str, Any]:
    if not path:
        return {}
    fp = Path(path)
    if not fp.exists():
        return {}
    try:
        text = fp.read_text(encoding="utf-8")
        if fp.suffix.lower() == ".toml":
            if tomllib is None:
                return {}
            obj = tomllib.loads(text)
        else:
            obj = json.loads(text)
    except Exception:
        return {}
    return normalize_field_mapping_config(obj if isinstance(obj, dict) else {})


def merge_header_aliases(config: dict[str, Any] | None) -> dict[str, list[str]]:
    merged = copy.deepcopy(DEFAULT_HEADER_ALIASES)
    overrides = ((config or {}).get("headerAliases") or {})
    if not isinstance(overrides, dict):
        return merged
    for key, raw_values in overrides.items():
        cleaned = _normalize_string_list([*merged.get(str(key), []), *(_normalize_string_list(raw_values))])
        merged[str(key)] = cleaned
    return merged


def normalize_relation(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if text in {"equal", "equals", "exact"}:
        return "equals"
    if text in {"associated", "associate", "related", "relation"}:
        return "associated"
    return text or "associated"


def get_output_field_mapping(config: dict[str, Any] | None, field_name: str) -> dict[str, Any]:
    base = copy.deepcopy(DEFAULT_OUTPUT_FIELD_MAPPINGS.get(field_name) or {"relation": "associated", "sourceFields": []})
    all_overrides = ((config or {}).get("outputFieldMappings") or {})
    overrides = (all_overrides.get(field_name) or {})
    if field_name == "category_ref_text" and not overrides:
        overrides = copy.deepcopy(all_overrides.get("account_ref_text") or {})
    if isinstance(overrides, dict):
        if "relation" in overrides:
            base["relation"] = normalize_relation(overrides.get("relation"))
        if "sourceFields" in overrides:
            raw = overrides.get("sourceFields") or []
            values = raw if isinstance(raw, list) else [raw]
            base["sourceFields"] = [str(value).strip() for value in values if str(value).strip()]
        for key, value in overrides.items():
            if key in {"relation", "sourceFields"}:
                continue
            base[key] = copy.deepcopy(value)
    base["relation"] = normalize_relation(base.get("relation"))
    return base


def get_mapping_source_values(recap: dict[str, Any], mapping: dict[str, Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for field_name in mapping.get("sourceFields") or []:
        text = str((recap or {}).get(str(field_name), "") or "").strip()
        if text:
            out.append({"field": str(field_name), "value": text})
    return out


def build_associated_text(recap: dict[str, Any], mapping: dict[str, Any]) -> str:
    return " | ".join(item["value"] for item in get_mapping_source_values(recap, mapping))
