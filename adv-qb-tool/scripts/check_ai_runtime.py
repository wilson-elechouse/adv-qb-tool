#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from canonical_support import script_path


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_runtime_config(path_text: str):
    path = Path(str(path_text or "").strip())
    if not path.exists():
        return {}
    try:
        return read_json(path)
    except Exception:
        return {}


def normalize_ai_cmd(ai_cmd: str, ai_runtime_config: str = "") -> str:
    cmd = str(ai_cmd or "").strip()
    if not cmd:
        return ""
    if re.match(r'^(python|py)(?:\.exe)?\s+["\']?scripts[\\/]+ai_bridge\.py["\']?\s*$', cmd, flags=re.IGNORECASE):
        built = [sys.executable, str(script_path("ai_bridge.py").resolve())]
        if ai_runtime_config:
            built += ["--runtime-config", str(Path(ai_runtime_config).resolve())]
        return subprocess.list2cmdline(built)
    return cmd


def resolve_ai_cmd(profile: str, runtime_config_path: str, explicit_ai_cmd: str):
    runtime = load_runtime_config(runtime_config_path)
    env_keys = {
        "codex": ["CODEX_AI_CMD", "ADV_QB_AI_CMD"],
        "openclaw": ["OPENCLAW_AI_CMD", "ADV_QB_AI_CMD"],
    }.get(profile, ["ADV_QB_AI_CMD", "OPENCLAW_AI_CMD", "CODEX_AI_CMD"])

    cmd = str(explicit_ai_cmd or "").strip()
    source = "arg"
    if not cmd:
        for key in env_keys:
            value = str(os.environ.get(key, "") or "").strip()
            if value:
                cmd = value
                source = f"env:{key}"
                break
    if not cmd:
        cmd = str(runtime.get("ai_cmd", "") or "").strip()
        source = "runtime_config"
    cmd = normalize_ai_cmd(cmd, runtime_config_path)
    return cmd, source, runtime


def check_ai_runtime(ai_cmd: str, timeout_seconds: int, category_ai_mode: str):
    mode = str(category_ai_mode or "per-record").strip() or "per-record"
    if mode == "batch-review":
        payload = {
            "task": "batch_category_review",
            "instruction": "Return strict JSON only with records:[{record_index, category_ref_text, confidence, top3, rationale}]",
            "shared_allowed_options": [
                "5702 Consultancy Fee",
                "5001 COS - Gasoline",
                "5408 Other General and Administrative Expenses",
            ],
            "records": [
                {
                    "record_index": 0,
                    "request_no": "TEST-001",
                    "status": "ready",
                    "review_reasons": ["non_authoritative_ai:heuristic_fallback"],
                    "source_inputs": {
                        "payment_detail_01_text": "Payment Type:Admin Payment | Net Amount-PHP:1000",
                        "payment_detail_02_text": "Which Supplier:Personal Supplier-Internal | Product:Service-Consultancy | Payment To:Employees Account",
                        "reason": "MANAGEMENT CONSULTANT FEE",
                    },
                    "program_result": {
                        "current_account": {
                            "value": "5702 Consultancy Fee",
                            "confidence": 0.91,
                            "source": "rule_keyword",
                            "alternatives": ["5702 Consultancy Fee", "5001 COS - Gasoline", "5408 Other General and Administrative Expenses"],
                        },
                        "unresolved": [],
                        "validation_issues": [],
                        "source_errors": [],
                    },
                }
            ],
        }
    else:
        payload = {
            "task": "category_judge",
            "inputs": {
                "payment_detail_01_text": "Payment Type:Admin Payment | Net Amount-PHP:1000",
                "payment_detail_02_text": "Which Supplier:Personal Supplier-Internal | Product:Service-Consultancy | Payment To:Employees Account",
                "reason": "MANAGEMENT CONSULTANT FEE",
                "options": [
                    "5702 Consultancy Fee",
                    "5001 COS - Gasoline",
                    "5408 Other General and Administrative Expenses",
                ],
                "instruction": "Return strict JSON only with category_ref_text, confidence, top3, rationale.",
            },
        }
    proc = subprocess.run(
        ai_cmd,
        input=json.dumps(payload, ensure_ascii=False),
        text=True,
        capture_output=True,
        shell=True,
        timeout=max(1, int(timeout_seconds)),
    )
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode != 0:
        return {
            "ai_ready": False,
            "returncode": proc.returncode,
            "stdout": stdout,
            "stderr": stderr,
            "error": f"ai_cmd_nonzero:{proc.returncode}",
        }
    try:
        obj = json.loads(stdout)
    except Exception as exc:
        return {
            "ai_ready": False,
            "returncode": proc.returncode,
            "stdout": stdout,
            "stderr": stderr,
            "error": f"ai_cmd_invalid_json:{exc}",
        }
    if mode == "batch-review":
        records = obj.get("records") if isinstance(obj, dict) else None
        first = records[0] if isinstance(records, list) and records else {}
        category = str((first or {}).get("category_ref_text") or "").strip()
        options = payload.get("shared_allowed_options") or []
        ai_ready = bool(category) and category in options
    else:
        category = str(obj.get("category_ref_text") or "").strip()
        options = payload["inputs"]["options"]
        ai_ready = bool(category) and category in options
    return {
        "ai_ready": ai_ready,
        "returncode": proc.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "result": obj,
        "error": "" if ai_ready else "ai_cmd_invalid_choice",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=["codex", "openclaw"], default=str(os.environ.get("ADV_QB_RUNTIME_PROFILE") or "codex").strip().lower() or "codex")
    ap.add_argument("--runtime-config", default=str(Path("references") / "config" / "ai-runtime.json"))
    ap.add_argument("--ai-cmd", default="")
    ap.add_argument("--category-ai-mode", choices=["per-record", "batch-review"], default="per-record")
    ap.add_argument("--strict-ai", action="store_true", help="return nonzero if AI is disabled or unusable")
    args = ap.parse_args()

    runtime_config_path = str(Path(args.runtime_config).resolve())
    ai_cmd, ai_cmd_source, runtime = resolve_ai_cmd(args.profile, runtime_config_path, args.ai_cmd)
    runtime_required = bool(runtime.get("required"))
    strict_ai = bool(args.strict_ai or runtime_required)

    if not ai_cmd:
        result = {
            "ok": not strict_ai,
            "profile": args.profile,
            "runtime_config": runtime_config_path,
            "ai_cmd_source": ai_cmd_source,
            "ai_cmd": "",
            "ai_required": strict_ai,
            "ai_ready": False,
            "status": "ai_disabled",
            "error": "ai_cmd_missing",
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        raise SystemExit(1 if strict_ai else 0)

    timeout_seconds = int(runtime.get("timeout_seconds", 180) or 180)
    check = check_ai_runtime(ai_cmd, timeout_seconds, args.category_ai_mode)
    ok = bool(check.get("ai_ready"))
    result = {
        "ok": ok if strict_ai else True,
        "profile": args.profile,
        "runtime_config": runtime_config_path,
        "ai_cmd_source": ai_cmd_source,
        "ai_cmd": ai_cmd,
        "ai_required": strict_ai,
        "ai_ready": ok,
        "status": "ready" if ok else "failed",
        **check,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()
