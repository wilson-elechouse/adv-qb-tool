#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

import requests


def create_session(base, cfg):
    s = requests.Session()
    if cfg.get("username") and cfg.get("password"):
        r = s.post(
            f"{base}/api/auth/login",
            json={"username": cfg["username"], "password": cfg["password"]},
            timeout=30,
        )
        r.raise_for_status()
    if cfg.get("tenant_id"):
        r = s.post(f"{base}/api/tenant/select", json={"tenantId": cfg["tenant_id"]}, timeout=30)
        r.raise_for_status()
    return s


def parse_conflict(err: Exception):
    if not isinstance(err, requests.HTTPError) or err.response is None:
        return None
    text = ""
    body = {}
    try:
        body = err.response.json() if err.response.text else {}
    except Exception:
        text = err.response.text or ""
    blob = f"{text} {json.dumps(body, ensure_ascii=False)}".lower()
    if "client_ref_not_unique" in blob or ("client" in blob and "ref" in blob and "unique" in blob):
        ex_id = body.get("existing_submission_id") or body.get("submission_id") or ((body.get("row") or {}).get("id"))
        return {
            "conflict": "client_ref_not_unique",
            "existing_submission_id": ex_id,
            "existing_view_path": body.get("existing_view_path"),
            "detail": body or text,
        }
    return None


def build_existing_view_url(base, conflict):
    existing_id = conflict.get("existing_submission_id")
    existing_view_path = conflict.get("existing_view_path")
    if existing_view_path:
        return f"{base}{existing_view_path}"
    if existing_id:
        return f"{base}/submissions/{existing_id}/edit"
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", required=True)
    ap.add_argument("--config", required=True)
    ap.add_argument("--confirmation-received", required=True)
    ap.add_argument(
        "--on-client-ref-conflict",
        default="block",
        help="block|ask|overwrite|abort (legacy values are treated as block)",
    )
    ap.add_argument("--overwrite-map", help="legacy option, ignored")
    args = ap.parse_args()

    if str(args.confirmation_received).lower() != "true":
        raise RuntimeError("confirmation_required")

    summary = json.loads(Path(args.summary).read_text(encoding="utf-8"))
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    base = cfg.get("base_url", "https://qb.uudc.us")
    sess = create_session(base, cfg)

    out = {"ok": True, "total": 0, "success": 0, "fail": 0, "results": []}
    for item in summary.get("results", []):
        out["total"] += 1
        m = json.loads(Path(item["match_file"]).read_text(encoding="utf-8"))
        if m.get("status") not in ["ready", "ready_with_blanks"]:
            out["fail"] += 1
            out["results"].append({"record_index": item.get("record_index"), "ok": False, "error": "match_not_ready"})
            continue
        draft = ((m.get("collector_payload") or {}).get("draft") or {})
        kind = m.get("kind", "bill")
        client_ref = str(draft.get("client_ref") or "").strip()
        if not client_ref:
            out["fail"] += 1
            out["results"].append({"record_index": item.get("record_index"), "ok": False, "error": "client_ref_required"})
            continue

        memo = "[AI_AGENT][adv-qbo-tool]"
        sid = None
        try:
            r = sess.post(f"{base}/api/submissions", json={"kind": kind, "client_ref": client_ref, "memo": memo}, timeout=30)
            r.raise_for_status()
            created = r.json()
            sid = created.get("row", {}).get("id")
            if not sid:
                raise RuntimeError("create_submission_missing_id")
        except Exception as e:
            conflict = parse_conflict(e)
            if not conflict:
                out["fail"] += 1
                out["results"].append({"record_index": item.get("record_index"), "ok": False, "error": str(e)})
                continue

            out["fail"] += 1
            out["results"].append({
                "record_index": item.get("record_index"),
                "ok": False,
                "needs_user_action": True,
                "needs_user_decision": True,
                "action_required": "delete_existing_submission",
                "error": "client_ref_not_unique",
                "client_ref": client_ref,
                "existing_submission_id": conflict.get("existing_submission_id"),
                "existing_view_url": build_existing_view_url(base, conflict),
                "message": "An existing submission with the same client_ref already exists. Delete the existing submission first, then retry.",
            })
            continue

        try:
            r = sess.put(f"{base}/api/submissions/{sid}", json={"client_ref": client_ref, "memo": memo, "payload": draft}, timeout=30)
            r.raise_for_status()
            rv = sess.post(f"{base}/api/submissions/{sid}/validate", json={}, timeout=30)
            rv.raise_for_status()
            rp = sess.post(f"{base}/api/submissions/{sid}/precheck", json={}, timeout=30)
            rp.raise_for_status()

            out["success"] += 1
            out["results"].append({"record_index": item.get("record_index"), "ok": True, "submission_id": sid, "client_ref": client_ref, "view_url": f"{base}/submissions/{sid}/edit"})
        except Exception as e:
            out["fail"] += 1
            out["results"].append({"record_index": item.get("record_index"), "ok": False, "error": str(e)})

    out_path = Path(args.summary).parent / "batch_submit_result.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "out": str(out_path.resolve()), "success": out["success"], "fail": out["fail"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
