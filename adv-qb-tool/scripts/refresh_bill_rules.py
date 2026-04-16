#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

import requests



def read_json(p: Path):
    return json.loads(p.read_text(encoding="utf-8-sig"))


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def parse_bool_flag(raw, default: bool = False) -> bool:
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def norm_tax_labels(raw):
    q = ((raw or {}).get("rules") or raw or {}).get("qboOptionDictionaries") or {}
    labels = []
    for item in q.get("taxCodes") or []:
        label = item.get("label") or item.get("key") or str(item or "")
        label = str(label).strip().lower()
        if label:
            labels.append(label)
    joined = " | ".join(labels)
    return {
        "wht_out_of_scope_found": bool(re.search(r"wht\s*-\s*out\s*of\s*scope", joined)),
        "vat_12_found": bool(re.search(r"vat\s*12%", joined)),
        "non_taxable_found": bool(re.search(r"non-?taxable", joined)),
    }


def response_json_text(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        return resp.text or ""


def create_session(base_url: str, username: str, password: str, tenant_id: str):
    sess = requests.Session()
    sess.headers.update({"Accept": "application/json", "User-Agent": "adv-qb-tool/collector-refresh"})
    if username and password:
        r = sess.post(
            f"{base_url}/api/auth/login",
            json={"username": username, "password": password},
            timeout=30,
        )
        if r.status_code == 401:
            raise RuntimeError("invalid_credentials")
        r.raise_for_status()
    if tenant_id:
        r = sess.post(
            f"{base_url}/api/tenant/select",
            json={"tenantId": tenant_id},
            timeout=30,
        )
        if r.status_code >= 400:
            detail = response_json_text(r)
            raise RuntimeError(f"tenant_select_failed:{detail}")
        r.raise_for_status()
    return sess


def fetch_rules(base_url: str, username: str, password: str, tenant_id: str):
    sess = create_session(base_url, username, password, tenant_id)
    r = sess.get(f"{base_url}/api/bill-rules", timeout=30)
    if r.status_code >= 400:
        detail = response_json_text(r)
        raise RuntimeError(f"bill_rules_fetch_failed:{detail}")
    r.raise_for_status()
    return r.json() if r.text else {}


def main():
    ap = argparse.ArgumentParser(description="Manual refresh of Collector bill-rules with snapshot + manifest")
    ap.add_argument("--config", required=True, help="company config json (base_url/username/password/tenant_id)")
    ap.add_argument("--out-dir", required=True, help="snapshot directory")
    ap.add_argument("--set-latest", default="true", help="write latest.json pointer copy (true/false)")
    ap.add_argument("--dry-run", nargs="?", const="true", default="false", help="fetch+compare hash only, do not write snapshot")
    ap.add_argument("--tenant-id", help="optional tenant override")
    args = ap.parse_args()

    cfg = read_json(Path(args.config))
    base_url = str(cfg.get("base_url", "https://qb.uudc.us")).rstrip("/")
    username = cfg.get("username", "")
    password = cfg.get("password", "")
    tenant_id = args.tenant_id or cfg.get("tenant_id", "")

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    latest_path = out_dir / "latest.json"
    manifest_path = out_dir / "manifest.json"

    try:
        rules = fetch_rules(base_url, username, password, tenant_id)
    except Exception as exc:
        print(json.dumps({
            "ok": False,
            "base_url": base_url,
            "tenant_id": tenant_id or None,
            "error": str(exc),
        }, ensure_ascii=False, indent=2))
        raise SystemExit(1)
    payload = json.dumps(rules, ensure_ascii=False, indent=2)
    new_hash = sha256_text(payload)

    old_hash = None
    if latest_path.exists():
        old_payload = latest_path.read_text(encoding="utf-8")
        old_hash = sha256_text(old_payload)

    changed = (new_hash != old_hash)

    taxonomy_check = norm_tax_labels(rules)
    if parse_bool_flag(args.dry_run):
        print(json.dumps({
            "ok": True,
            "mode": "dry_run",
            "changed": changed,
            "old_hash": old_hash,
            "new_hash": new_hash,
            "tenant_id": tenant_id,
            "base_url": base_url,
            "taxonomy_check": taxonomy_check,
        }, ensure_ascii=False, indent=2))
        return

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    snap_path = out_dir / f"bill-rules.{stamp}.json"
    snap_path.write_text(payload, encoding="utf-8")

    set_latest = parse_bool_flag(args.set_latest, default=True)
    if set_latest:
        latest_path.write_text(payload, encoding="utf-8")

    manifest = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "tenant_id": tenant_id,
        "base_url": base_url,
        "latest_snapshot": str(snap_path),
        "latest_hash": new_hash,
        "previous_hash": old_hash,
        "changed": changed,
        "set_latest": set_latest,
        "source": "manual_refresh",
        "taxonomy_check": taxonomy_check,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "ok": True,
        "changed": changed,
        "snapshot": str(snap_path),
        "latest": str(latest_path) if latest_path.exists() else None,
        "manifest": str(manifest_path),
        "hash": new_hash,
        "taxonomy_check": taxonomy_check,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
