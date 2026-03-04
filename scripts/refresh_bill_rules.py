#!/usr/bin/env python3
import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

from urllib import request as urlrequest



def read_json(p: Path):
    return json.loads(p.read_text(encoding="utf-8"))


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _json_request(url: str, method: str = "GET", body=None, cookie: str = ""):
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urlrequest.Request(url, data=data, method=method)
    req.add_header("content-type", "application/json")
    if cookie:
        req.add_header("cookie", cookie)
    with urlrequest.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        set_cookie = resp.headers.get("set-cookie", "")
        c = set_cookie.split(";")[0] if set_cookie else ""
        return (json.loads(raw) if raw else {}), c


def fetch_rules(base_url: str, username: str, password: str, tenant_id: str):
    cookie = ""
    if username and password:
        _, c = _json_request(f"{base_url}/api/auth/login", method="POST", body={"username": username, "password": password}, cookie=cookie)
        cookie = "; ".join([x for x in [cookie, c] if x])
    if tenant_id:
        _, c = _json_request(f"{base_url}/api/tenant/select", method="POST", body={"tenantId": tenant_id}, cookie=cookie)
        cookie = "; ".join([x for x in [cookie, c] if x])
    data, _ = _json_request(f"{base_url}/api/bill-rules", method="GET", cookie=cookie)
    return data


def main():
    ap = argparse.ArgumentParser(description="Manual refresh of Collector bill-rules with snapshot + manifest")
    ap.add_argument("--config", required=True, help="company config json (base_url/username/password/tenant_id)")
    ap.add_argument("--out-dir", required=True, help="snapshot directory")
    ap.add_argument("--set-latest", default="true", help="write latest.json pointer copy (true/false)")
    ap.add_argument("--dry-run", action="store_true", help="fetch+compare hash only, do not write snapshot")
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

    rules = fetch_rules(base_url, username, password, tenant_id)
    payload = json.dumps(rules, ensure_ascii=False, sort_keys=True)
    new_hash = sha256_text(payload)

    old_hash = None
    if latest_path.exists():
        old_payload = latest_path.read_text(encoding="utf-8")
        old_hash = sha256_text(old_payload)

    changed = (new_hash != old_hash)

    if args.dry_run:
        print(json.dumps({
            "ok": True,
            "mode": "dry_run",
            "changed": changed,
            "old_hash": old_hash,
            "new_hash": new_hash,
            "tenant_id": tenant_id,
            "base_url": base_url,
        }, ensure_ascii=False, indent=2))
        return

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    snap_path = out_dir / f"bill-rules.{stamp}.json"
    snap_path.write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")

    if str(args.set_latest).lower() == "true":
        latest_path.write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")

    manifest = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "tenant_id": tenant_id,
        "base_url": base_url,
        "latest_snapshot": str(snap_path),
        "latest_hash": new_hash,
        "previous_hash": old_hash,
        "changed": changed,
        "set_latest": str(args.set_latest).lower() == "true",
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "ok": True,
        "changed": changed,
        "snapshot": str(snap_path),
        "latest": str(latest_path) if latest_path.exists() else None,
        "manifest": str(manifest_path),
        "hash": new_hash,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
