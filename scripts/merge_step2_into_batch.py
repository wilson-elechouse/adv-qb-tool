#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def read_json(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))


def write_json(p, obj):
    Path(p).write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step2", required=True, help="step2_ai_judge.json")
    ap.add_argument("--summary", required=True, help="batch_match_summary.json")
    ap.add_argument("--auto-threshold", type=float, default=0.85)
    ap.add_argument("--confirm-threshold", type=float, default=0.65)
    args = ap.parse_args()

    step2 = read_json(args.step2)
    summary = read_json(args.summary)

    idx_to_ai = {}
    for r in step2.get("records", []):
        idx_to_ai[int(r.get("record_index", 0))] = r.get("category_ai") or {}

    for item in summary.get("results", []):
        mpath = item.get("match_file")
        if not mpath:
            continue
        p = Path(mpath)
        if not p.exists():
            continue
        m = read_json(str(p))
        ridx = int(item.get("record_index", 0))
        ai = idx_to_ai.get(ridx, {})
        if ai:
            f = m.setdefault("fields", {}).setdefault("category_ref_text", {})
            if not f.get("value"):
                f["value"] = ai.get("category_ref_text") or ""
            f["ai_rationale"] = ai.get("rationale") or ""
            if ai.get("top3"):
                f["alternatives"] = ai.get("top3")
            conf = None
            if ai.get("confidence") is not None:
                conf = float(ai.get("confidence") or 0)
                f["confidence"] = conf
                f["source"] = "ai+dictionary"

            # Confidence policy:
            # >= auto-threshold: auto pass
            # [confirm-threshold, auto-threshold): keep suggestion, require user confirmation
            # < confirm-threshold: require confirmation (suggestion only)
            if not f.get("value"):
                f["needs_user_confirmation"] = True
            elif conf is None:
                f["needs_user_confirmation"] = True
            elif conf >= float(args.auto_threshold):
                f["needs_user_confirmation"] = False
            else:
                f["needs_user_confirmation"] = True

            if conf is not None:
                if conf >= float(args.auto_threshold):
                    f["confidence_band"] = "auto_pass"
                elif conf >= float(args.confirm_threshold):
                    f["confidence_band"] = "confirm_recommended"
                else:
                    f["confidence_band"] = "confirm_required"

            # sync payload line category fields deterministically
            draft = (m.get("collector_payload") or {}).get("draft") or {}
            lines = draft.get("lines") or []
            for ln in lines:
                if str((ln.get("meta") or {}).get("kind") or "business") != "business":
                    continue
                ln["category_ref_text"] = f.get("value", "")
                ln["account_ref_text"] = f.get("value", "")
                if f.get("ref_id"):
                    ln["account_ref_id"] = f.get("ref_id")
                elif "account_ref_id" in ln:
                    ln.pop("account_ref_id", None)

            # unresolved refresh
            unresolved = [k for k, v in (m.get("fields") or {}).items() if isinstance(v, dict) and v.get("needs_user_confirmation")]
            m.setdefault("interaction", {})["unresolved"] = unresolved
            if unresolved:
                m["status"] = "needs_user_confirmation"
                m["ready_to_upload"] = False
            else:
                m["status"] = "ready"
                m["ready_to_upload"] = True

        write_json(str(p), m)

    # sync summary statuses from updated match files
    for item in summary.get("results", []):
        mpath = item.get("match_file")
        if not mpath or not Path(mpath).exists():
            continue
        m = read_json(mpath)
        item["status"] = m.get("status", item.get("status"))
        item["unresolved"] = (m.get("interaction") or {}).get("unresolved", item.get("unresolved", []))

    write_json(args.summary, summary)
    print(json.dumps({"ok": True, "summary": args.summary}, ensure_ascii=False))


if __name__ == "__main__":
    main()
