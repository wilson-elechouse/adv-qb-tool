#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path



def run(cmd, cwd=None):
    p = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "command failed").strip())
    return (p.stdout or "").strip()


def write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def parse_json_output(text: str):
    s = str(text or "").strip()
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        pass
    lines = [x for x in s.splitlines() if x.strip()]
    for ln in reversed(lines):
        try:
            return json.loads(ln)
        except Exception:
            continue
    return {}


def _sha256_file(p: Path):
    return hashlib.sha256(p.read_text(encoding="utf-8").encode("utf-8")).hexdigest()


def fetch_live_bill_rules(config_path: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    raw = run([
        "node", "skills/adv-qbo-tool/scripts/refresh_bill_rules.mjs",
        "--config", str(config_path.resolve()),
        "--out-dir", str(out_dir.resolve()),
        "--set-latest", "true",
    ])
    obj = json.loads(raw)
    if not obj.get("ok"):
        raise RuntimeError(f"refresh_bill_rules_failed:{raw}")
    return {
        "snapshot": Path(obj.get("snapshot")),
        "latest": Path(obj.get("latest")) if obj.get("latest") else (out_dir / "latest.json"),
        "manifest": Path(obj.get("manifest")) if obj.get("manifest") else (out_dir / "manifest.json"),
        "hash": obj.get("hash", ""),
        "taxonomy_check": obj.get("taxonomy_check") or {},
    }


def needs_live_refresh(summary_obj):
    for item in summary_obj.get("results", []):
        unresolved = set(item.get("unresolved") or [])
        if unresolved.intersection({"vendor_ref_text", "category_ref_text", "tax_code_ref_text", "location_ref_text"}):
            return True
    return False


def resolve_effective_rules(input_rules: Path, cache_dir: Path, manual_snapshot: str = ""):
    if manual_snapshot:
        p = Path(manual_snapshot).resolve()
        if p.exists():
            return {"path": p, "source": "manual_refresh", "hash": _sha256_file(p)}
    latest = cache_dir / "latest.json"
    if latest.exists():
        return {"path": latest.resolve(), "source": "local_latest", "hash": _sha256_file(latest)}
    return {"path": input_rules.resolve(), "source": "local_input", "hash": _sha256_file(input_rules)}


def is_ttl_expired(path: Path, ttl_seconds: int):
    if not path.exists():
        return True
    age = datetime.now().timestamp() - path.stat().st_mtime
    return age > max(0, ttl_seconds)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--bill-rules", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--chunk-size", type=int, default=10)
    ap.add_argument("--confirmed", action="store_true")
    ap.add_argument("--config")
    ap.add_argument("--ai-cmd", help="AI command for Step2 category judge (optional if configured in ai-runtime.json or OPENCLAW_AI_CMD)")
    ap.add_argument("--ai-runtime-config", default="skills/adv-qbo-tool/references/config/ai-runtime.json")
    ap.add_argument("--history", help="optional confirmed category history json")
    ap.add_argument("--auto-threshold", type=float, default=0.85)
    ap.add_argument("--confirm-threshold", type=float, default=0.65)
    ap.add_argument("--rules-cache-dir", help="optional directory for timestamped live bill-rules snapshots")
    ap.add_argument("--rules-ttl-seconds", type=int, default=21600, help="auto refresh ttl for latest rules snapshot")
    ap.add_argument("--manual-rules-snapshot", help="manual snapshot path with highest priority")
    args = ap.parse_args()

    workdir = Path(args.dir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    state_path = workdir / "workflow_state.json"

    # Resolve mandatory AI command (AI required, no fallback)
    ai_cmd = args.ai_cmd or os.environ.get("OPENCLAW_AI_CMD", "")
    if not ai_cmd and args.ai_runtime_config and Path(args.ai_runtime_config).exists():
        try:
            arc = read_json(Path(args.ai_runtime_config))
            ai_cmd = str(arc.get("ai_cmd", "") or "").strip()
        except Exception:
            ai_cmd = ai_cmd
    if not ai_cmd:
        ai_cmd = "python skills/adv-qbo-tool/scripts/ai_bridge.py"
    if not ai_cmd:
        raise RuntimeError("ai_required_missing: configure --ai-cmd or OPENCLAW_AI_CMD or references/config/ai-runtime.json:ai_cmd")

    cache_root = Path(args.rules_cache_dir).resolve() if args.rules_cache_dir else (workdir / "rules_cache")

    state = {
        "state": "S1_PARSE_IDENTIFY",
        "inputs": {
            "file": str(Path(args.file).resolve()),
            "bill_rules": str(Path(args.bill_rules).resolve()),
            "config": str(Path(args.config).resolve()) if args.config else None,
            "manual_rules_snapshot": str(Path(args.manual_rules_snapshot).resolve()) if args.manual_rules_snapshot else None,
            "rules_cache_dir": str(cache_root),
        },
        "artifacts": {},
        "metrics": {},
        "flags": {"confirmed": bool(args.confirmed)},
        "error": None,
    }

    try:
        # S1
        parse_out = workdir / "parse_result.json"
        run([
            "python", "skills/adv-qbo-tool/scripts/parse_payment_request_xlsx.py",
            "--file", state["inputs"]["file"],
            "--out", str(parse_out),
        ])
        parsed = read_json(parse_out)
        if not parsed.get("ok"):
            raise RuntimeError(f"parse_failed:{parsed.get('error','unknown')}")
        state["artifacts"]["parse_result"] = str(parse_out)
        state["state"] = "S2_MATCH_BUILD"
        write_json(state_path, state)

        # S2 (AI-judge facade + deterministic batch build)
        # priority: manual snapshot > cache latest > input bill-rules
        effective = resolve_effective_rules(Path(state["inputs"]["bill_rules"]), cache_root, state["inputs"].get("manual_rules_snapshot") or "")

        # TTL auto-refresh when using cached/latest path and config is available
        if args.config and effective["source"] in {"local_latest", "local_input"} and is_ttl_expired(cache_root / "latest.json", args.rules_ttl_seconds):
            try:
                refreshed = fetch_live_bill_rules(Path(args.config), cache_root)
                effective = {"path": refreshed["latest"], "source": "live_ttl_refresh", "hash": refreshed["hash"]}
                state["artifacts"]["live_bill_rules_snapshot"] = str(refreshed["snapshot"])
                state["artifacts"]["rules_manifest"] = str(refreshed["manifest"])
                state["artifacts"]["taxonomy_check"] = refreshed["taxonomy_check"]
            except Exception:
                # keep effective local rules if refresh fails
                pass

        ai_judge_out = workdir / "step2_ai_judge.json"
        step2_cmd = [
            "python", "skills/adv-qbo-tool/scripts/step2_match.py",
            "--parsed", str(parse_out),
            "--bill-rules", str(effective["path"]),
            "--out", str(ai_judge_out),
        ]
        step2_cmd += ["--ai-cmd", ai_cmd, "--allow-fallback", "false"]
        if args.history:
            step2_cmd += ["--history", args.history]
        step2_started = time.perf_counter()
        run(step2_cmd)
        step2_elapsed_ms = round((time.perf_counter() - step2_started) * 1000.0, 2)
        state["artifacts"]["step2_ai_judge"] = str(ai_judge_out)
        try:
            step2_obj = read_json(ai_judge_out)
            state["metrics"]["category_judge"] = {
                "subprocess_duration_ms": step2_elapsed_ms,
                **(step2_obj.get("metrics") or {}),
            }
        except Exception:
            state["metrics"]["category_judge"] = {"subprocess_duration_ms": step2_elapsed_ms}
        write_json(state_path, state)

        out_dir = workdir / "batch"
        step2_build_cmd = [
            "python", "skills/adv-qbo-tool/scripts/step2_batch_build.py",
            "--parsed", str(parse_out),
            "--bill-rules", str(effective["path"]),
            "--outDir", str(out_dir),
            "--chunk-size", str(max(1, args.chunk_size)),
            "--step2", str(ai_judge_out),
            "--rules-source", effective["source"],
            "--rules-snapshot", str(effective["path"]),
            "--rules-hash", effective["hash"],
        ]
        run(step2_build_cmd)
        summary = out_dir / "batch_match_summary.json"
        if not summary.exists():
            raise RuntimeError("batch_match_summary_missing")
        s = read_json(summary)

        # Cache-first strategy: only pull live rules when unresolved/low-confidence fields remain
        live_rules_path = None
        if args.config and needs_live_refresh(s):
            try:
                refreshed = fetch_live_bill_rules(Path(args.config), cache_root)
                live_rules_path = refreshed["snapshot"]
                run(step2_build_cmd + [
                    "--live-rules", str(live_rules_path),
                    "--rules-source", "live_retry",
                    "--rules-snapshot", str(live_rules_path),
                    "--rules-hash", refreshed["hash"],
                ])
                s = read_json(summary)
                state["artifacts"]["live_bill_rules_snapshot"] = str(live_rules_path)
                state["artifacts"]["rules_manifest"] = str(refreshed["manifest"])
                state["artifacts"]["taxonomy_check"] = refreshed["taxonomy_check"]
            except Exception:
                # keep local-cache results if live refresh fails
                live_rules_path = None

        # Merge Step2 AI category suggestions into batch match outputs
        run([
            "python", "skills/adv-qbo-tool/scripts/merge_step2_into_batch.py",
            "--step2", str(ai_judge_out),
            "--summary", str(summary),
            "--auto-threshold", str(args.auto_threshold),
            "--confirm-threshold", str(args.confirm_threshold),
        ])

        state["artifacts"]["batch_match_summary"] = str(summary)
        state["artifacts"]["effective_bill_rules"] = str(effective["path"])
        state["artifacts"]["effective_bill_rules_source"] = effective["source"]
        state["artifacts"]["effective_bill_rules_hash"] = effective["hash"]
        first_match = s.get("results", [{}])[0].get("match_file")
        if not first_match:
            raise RuntimeError("batch_first_match_missing")
        state["artifacts"]["first_match_result"] = str(Path(first_match).resolve())
        state["state"] = "S3_CONFIRM_RENDER"
        write_json(state_path, state)

        # S3 (python recap renderer)
        recap_out = workdir / "confirmation_recap.json"
        run([
            "python", "skills/adv-qbo-tool/scripts/step3_render_recap.py",
            "--summary", str(summary),
            "--out", str(recap_out),
        ])
        state["artifacts"]["confirmation_recap"] = str(recap_out)
        state["state"] = "WAIT_CONFIRMATION"
        write_json(state_path, state)

        # Stop at confirmation unless explicitly confirmed.
        if not args.confirmed:
            print(json.dumps({
                "ok": True,
                "state": state["state"],
                "state_file": str(state_path),
                "recap": str(recap_out),
            }, ensure_ascii=False, indent=2))
            return

        # S4/S5: create -> validate -> precheck submit chain
        if not args.config:
            raise RuntimeError("confirmed_submit_requires_config")
        state["state"] = "S4_SUBMIT_PRECHECK"
        write_json(state_path, state)

        submit_raw = run([
            "python", "skills/adv-qbo-tool/scripts/step4_submit.py",
            "--summary", str(summary),
            "--config", str(Path(args.config).resolve()),
            "--confirmation-received", "true",
            "--on-client-ref-conflict", "block",
        ])
        submit_obj = parse_json_output(submit_raw)
        submit_out = submit_obj.get("out")
        if submit_out:
            state["artifacts"]["batch_submit_result"] = str(Path(submit_out).resolve())

        state["state"] = "DONE"
        write_json(state_path, state)

        print(json.dumps({
            "ok": True,
            "state": state["state"],
            "state_file": str(state_path),
            "recap": str(recap_out),
            "submit_result": state["artifacts"].get("batch_submit_result"),
            "submit_summary": submit_obj or None,
        }, ensure_ascii=False, indent=2))

    except Exception as e:
        state["state"] = "ERROR"
        state["error"] = str(e)
        write_json(state_path, state)
        raise


if __name__ == "__main__":
    main()
