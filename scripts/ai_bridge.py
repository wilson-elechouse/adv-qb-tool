#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys
import shutil
import uuid
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
WORKSPACE_DIR = SCRIPT_DIR.parents[2]


def load_runtime_config():
    candidates = [
        SKILL_DIR / "references" / "config" / "ai-runtime.json",
        Path("skills/adv-qbo-tool/references/config/ai-runtime.json"),
    ]
    for cfg_path in candidates:
        if cfg_path.exists():
            try:
                return json.loads(cfg_path.read_text(encoding="utf-8"))
            except Exception:
                return {}
    return {}


def resolve_openclaw_bin():
    cands = [
        shutil.which("openclaw"),
        shutil.which("openclaw.cmd"),
    ]
    appdata = os.environ.get("APPDATA")
    if appdata:
        cands.append(str(Path(appdata) / "npm" / "openclaw.cmd"))
    for c in cands:
        if c and Path(c).exists():
            return c
    return "openclaw"


def sanitize_for_cmd(message: str):
    # prevent cmd metachar interpretation when calling .cmd wrappers
    repl = {
        "|": " / ",
        "&": " and ",
        "<": "(",
        ">": ")",
        "^": "",
        "\r": " ",
        "\n": " ",
    }
    s = str(message or "")
    for k, v in repl.items():
        s = s.replace(k, v)
    return s


def _clip(s: str, n: int) -> str:
    t = str(s or "")
    return t if len(t) <= n else (t[:n] + "…")


def run_openclaw_agent(message: str, session_id: str, thinking: str = "low", timeout_sec: int = 120, local: bool = True, agent_id: str = ""):
    bin_name = resolve_openclaw_bin()
    safe_message = sanitize_for_cmd(message)
    cmd = [
        bin_name, "agent",
        "--session-id", session_id,
        "--message", safe_message,
        "--json",
        "--thinking", thinking,
    ]
    if str(agent_id or "").strip():
        cmd.extend(["--agent", str(agent_id).strip()])
    if local:
        cmd.append("--local")

    if str(bin_name).lower().endswith('.cmd'):
        # cmd shim path on Windows (avoid shell quoting with huge message)
        p = subprocess.run(["cmd", "/c", bin_name, *cmd[1:]], capture_output=True, timeout=timeout_sec)
    else:
        p = subprocess.run(cmd, capture_output=True, timeout=timeout_sec)

    stdout = (p.stdout or b"").decode("utf-8", "ignore").strip()
    stderr = (p.stderr or b"").decode("utf-8", "ignore").strip()
    if p.returncode != 0:
        raise RuntimeError((stderr or stdout or "openclaw agent failed").strip())
    return stdout


def _parse_json_like(text: str):
    try:
        return json.loads(text)
    except Exception:
        # best-effort fenced/json-fragment extraction
        l = text.find("{")
        r = text.rfind("}")
        if l >= 0 and r > l:
            return json.loads(text[l:r+1])
        raise


def _extract_openclaw_payload_text(obj):
    if not isinstance(obj, dict):
        return ""
    payloads = obj.get("payloads")
    if not isinstance(payloads, list):
        return ""
    texts = []
    for item in payloads:
        if not isinstance(item, dict):
            continue
        txt = str(item.get("text", "") or "").strip()
        if txt:
            texts.append(txt)
    return "\n".join(texts).strip()


def extract_json(text: str, depth: int = 0):
    text = (text or "").strip()
    if not text:
        raise RuntimeError("ai_bridge_empty_output")
    if depth > 3:
        raise RuntimeError("ai_bridge_json_unwrap_depth")
    obj = _parse_json_like(text)
    payload_text = _extract_openclaw_payload_text(obj)
    if payload_text:
        return extract_json(payload_text, depth + 1)
    return obj


def extract_first_int(text: str):
    m = re.search(r"\b(\d{1,6})\b", text or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def resolve_choice_number(raw_num, options, text_hint=""):
    """Resolve model numeric output to a valid option index (1-based).
    Supports:
    - direct index (1..N)
    - account-like code numbers in labels (e.g., 5001 -> option whose label contains 5001)
    - text hint containing full/partial label
    """
    n = None
    try:
        if raw_num is not None:
            n = int(float(raw_num))
    except Exception:
        n = None

    if n is not None and 1 <= n <= len(options):
        return n

    if n is not None:
        needle = str(n)
        for i, o in enumerate(options, start=1):
            if needle in str(o):
                return i

    hint = (text_hint or "").strip().lower()
    if hint:
        # exact contains mapping
        for i, o in enumerate(options, start=1):
            lo = str(o).strip().lower()
            if lo and (lo in hint or hint in lo):
                return i

    return None


def write_debug(debug_dir: str, name: str, content: str):
    if not debug_dir:
        return
    p = Path(debug_dir)
    if not p.is_absolute():
        p = WORKSPACE_DIR / p
    p.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    (p / f"{ts}-{name}.txt").write_text(content or "", encoding="utf-8")


def main():
    raw = sys.stdin.read()
    if not raw.strip():
        raise RuntimeError("ai_bridge_missing_stdin_json")
    payload = json.loads(raw)

    cfg = load_runtime_config()
    thinking = os.environ.get("OPENCLAW_AI_THINKING") or cfg.get("thinking") or "low"
    local = str(os.environ.get("OPENCLAW_AI_LOCAL", cfg.get("local", True))).lower() in {"1", "true", "yes", "on"}
    stateless = str(os.environ.get("OPENCLAW_AI_STATELESS", cfg.get("stateless", True))).lower() in {"1", "true", "yes", "on"}
    agent_id = os.environ.get("OPENCLAW_AI_AGENT_ID") or cfg.get("agent_id") or ""
    base_session = os.environ.get("OPENCLAW_AI_SESSION_ID") or cfg.get("session_id") or "adv-qbo-ai-judge"
    session_prefix = os.environ.get("OPENCLAW_AI_SESSION_PREFIX") or cfg.get("session_prefix") or base_session
    session_id = f"{session_prefix}-{uuid.uuid4().hex[:10]}" if stateless else base_session

    task = payload.get("task", "category_judge")
    inputs = payload.get("inputs", {})
    max_retries = int(cfg.get("max_retries", 3) or 3)
    debug_dir = str(cfg.get("debug_dir", "") or "")

    if task != "category_judge":
        raise RuntimeError(f"ai_bridge_unsupported_task:{task}")

    pd01 = _clip(str(inputs.get("payment_detail_01_text", "")), 700)
    pd02 = _clip(str(inputs.get("payment_detail_02_text", "")), 700)
    reason = _clip(str(inputs.get("reason", "")), 350)
    options = [str(x) for x in (inputs.get("options") or []) if str(x).strip()]
    # Step2 decision prompt: history disabled by request

    # prevent Windows command-line overflow: trim option payload size
    max_options = int(cfg.get("max_options", 80) or 80)
    options = options[:max_options]
    short_options = [_clip(o, 90) for o in options]

    numbered = [{"no": i + 1, "label": str(o)} for i, o in enumerate(short_options)]
    option_lines = " ; ".join([f"{x['no']}) {x['label']}" for x in numbered])

    base_prompt = (
        "Select ONE accounting category from allowed options. "
        "Use PD01 + PD02 + Reason jointly. "
        "Return STRICT JSON ONLY with keys category_number, confidence, top3_numbers, rationale. "
        "If JSON fails, return ONE integer only. "
        f"PD01={pd01} ; PD02={pd02} ; Reason={reason} ; Options={option_lines}"
    )

    obj = {}
    last_text = ""
    attempts_used = 0
    for attempt in range(1, max_retries + 1):
        attempts_used = attempt
        if attempt == 1:
            prompt = base_prompt
        elif attempt == 2:
            prompt = (
                "Return ONLY this JSON: "
                "{\"category_number\": <int>, \"confidence\": <0-1>, \"top3_numbers\": [int,int,int], \"rationale\": \"text\"}. "
                f"Options: {json.dumps(numbered, ensure_ascii=False)}. "
                f"Reason: {reason}."
            )
        else:
            prompt = (
                "Return ONLY ONE INTEGER (no words): the option number. "
                f"Options: {json.dumps(numbered, ensure_ascii=False)}."
            )

        out_text = run_openclaw_agent(prompt, session_id=session_id, thinking=thinking, local=local, agent_id=agent_id)
        last_text = out_text
        write_debug(debug_dir, f"attempt{attempt}-raw", out_text)

        if attempt < 3:
            try:
                obj = extract_json(out_text)
            except Exception:
                continue
            if isinstance(obj, dict) and any(k in obj for k in ("category_number", "category_ref_text", "top3_numbers", "top3")):
                break
        else:
            n = extract_first_int(out_text)
            if n is not None:
                obj = {"category_number": n, "confidence": 0.55, "top3_numbers": [n], "rationale": "numeric-only retry"}
                break

    if not any(k in obj for k in ("category_number", "category_ref_text", "top3_numbers", "top3")):
        raise RuntimeError("ai_bridge_missing_category")

    # Preferred: numbered outputs
    n = obj.get("category_number")
    top_nums = obj.get("top3_numbers") or []

    cat = ""
    top3 = []
    if options and isinstance(n, (int, float, str)):
        ni = resolve_choice_number(n, options)
        if ni is not None:
            cat = str(options[ni - 1])

    if options and isinstance(top_nums, list):
        for x in top_nums:
            xi = resolve_choice_number(x, options)
            if xi is not None:
                top3.append(str(options[xi - 1]))

    # Backward-compatible fallback: category_ref_text/top3 text
    if not cat:
        cat = str(obj.get("category_ref_text", "")).strip()
    if not top3:
        top3 = [str(x) for x in (obj.get("top3") or []) if str(x).strip()]

    # Last-chance numeric/text resolve from raw model output
    if not cat and options:
        n2 = extract_first_int(last_text)
        r2 = resolve_choice_number(n2, options, text_hint=last_text)
        if r2 is not None:
            cat = str(options[r2 - 1])
            top3 = [cat]

    # Corrective AI retry: force valid index domain only (avoid account-code confusion)
    if not cat and options:
        idx_domain = list(range(1, len(options) + 1))
        short = [{"index": i + 1, "label": str(o)} for i, o in enumerate(options[: min(len(options), 80)])]
        for _ in range(2):
            fix_prompt = (
                "Your previous answer used an invalid number domain. "
                "Choose ONLY one valid option index from INDEX DOMAIN. "
                "Do NOT use account code from labels. "
                "Return ONE integer only. "
                f"INDEX DOMAIN: {idx_domain[:80]}{' ...' if len(idx_domain) > 80 else ''}. "
                f"OPTIONS PREVIEW: {json.dumps(short, ensure_ascii=False)}"
            )
            t = run_openclaw_agent(fix_prompt, session_id=session_id, thinking=thinking, local=local, agent_id=agent_id)
            write_debug(debug_dir, "corrective-raw", t)
            n3 = extract_first_int(t)
            if n3 is not None and 1 <= n3 <= len(options):
                cat = str(options[n3 - 1])
                top3 = [cat]
                break

    if not cat:
        raise RuntimeError("ai_bridge_missing_category")

    if options:
        # normalize to options (case-insensitive exact first)
        opt_map = {str(o).strip().lower(): str(o).strip() for o in options}
        if cat.lower() in opt_map:
            cat = opt_map[cat.lower()]
        if cat not in options:
            # deterministic nearest fallback inside option set
            def score(v):
                vs = set(v.lower().split())
                cs = set(cat.lower().split())
                return len(vs & cs)
            cat = sorted(options, key=score, reverse=True)[0] if options else cat

        top3_norm = []
        for t in top3:
            k = str(t).strip().lower()
            if k in opt_map:
                top3_norm.append(opt_map[k])
        top3 = top3_norm

    if cat and cat not in top3:
        top3 = [cat] + top3
    top3 = top3[:3]

    conf = float(obj.get("confidence", 0) or 0)
    conf = max(0.0, min(1.0, conf))

    result = {
        "category_ref_text": cat,
        "confidence": conf,
        "top3": top3,
        "rationale": str(obj.get("rationale", "openclaw_agent")),
        "provider": "openclaw_local_agent",
        "agent_id": agent_id or None,
        "session_id": session_id,
        "stateless": stateless,
        "attempts": attempts_used,
    }
    sys.stdout.write(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        sys.stderr.write(str(e))
        sys.exit(1)
