#!/usr/bin/env python3
"""
Step-2 matcher (Python facade).
Current production path uses build_match_batch.mjs for deterministic compatibility.
This file provides an AI-judge extension point for category decisions and outputs
structured JSON for workflow consumption.
"""

import argparse
import json
import subprocess
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import re


@dataclass
class CategoryJudgeResult:
    category_ref_text: str
    confidence: float
    top3: List[str]
    rationale: str


MONTH_TOKENS = {
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
    "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec",
}

NOISE_TOKENS = {
    "payment", "payments", "history", "feishu", "total", "amount", "gross", "net", "php",
    "bank", "account", "info", "name", "method", "term", "which", "client",
}


def _norm_tokens(text: str) -> List[str]:
    t = (text or "").lower()
    t = re.sub(r"[^a-z0-9\s]+", " ", t)
    return [x for x in t.split() if len(x) >= 2]


def _jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(1, len(sa | sb))


def _extract_pipe_field(text: str, label: str) -> str:
    m = re.search(rf"{re.escape(label)}\s*:\s*([^|]+)", text or "", flags=re.IGNORECASE)
    if not m:
        return ""
    return " ".join(_feature_tokens(m.group(1)))


def _feature_tokens(text: str) -> List[str]:
    t = str(text or "").lower()
    t = re.sub(r"payment\s+history\s*:[\s\S]*$", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\bfeishu\s*id\b", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"[^a-z0-9\s]+", " ", t)
    out = []
    for tok in t.split():
        if len(tok) < 2:
            continue
        if any(ch.isdigit() for ch in tok):
            continue
        if tok in MONTH_TOKENS or tok in NOISE_TOKENS:
            continue
        out.append(tok)
    return out


def _reason_tokens(text: str) -> List[str]:
    head = re.split(r"payment\s+history\s*:", str(text or ""), maxsplit=1, flags=re.IGNORECASE)[0]
    lines = []
    for line in head.splitlines():
        if "total" in line.lower() and re.search(r"\d", line):
            continue
        lines.append(line)
    return _feature_tokens(" ".join(lines))


def _build_reuse_features(pd01_text: str, pd02_text: str, reason: str) -> Dict[str, Any]:
    reason_toks = _reason_tokens(reason)
    return {
        "payment_type": _extract_pipe_field(pd01_text, "Payment Type"),
        "supplier": _extract_pipe_field(pd02_text, "Which Supplier"),
        "product": _extract_pipe_field(pd02_text, "Product"),
        "payment_to": _extract_pipe_field(pd02_text, "Payment To"),
        "reason_tokens": reason_toks,
        "reason_signature": " ".join(sorted(set(reason_toks))),
    }


def _find_reuse_candidate(features: Dict[str, Any], cache: List[Dict[str, Any]], min_confidence: float, reason_min_jaccard: float):
    best = None
    best_score = -1.0
    for cand in cache:
        result = cand.get("result")
        if not isinstance(result, CategoryJudgeResult):
            continue
        if float(result.confidence or 0) < float(min_confidence):
            continue

        matched_fields = []
        conflict = False
        for field in ("payment_type", "supplier", "product", "payment_to"):
            cur_val = str(features.get(field, "") or "").strip()
            old_val = str((cand.get("features") or {}).get(field, "") or "").strip()
            if cur_val and old_val:
                if cur_val != old_val:
                    conflict = True
                    break
                matched_fields.append(field)
        if conflict:
            continue

        cur_reason = features.get("reason_tokens") or []
        old_reason = (cand.get("features") or {}).get("reason_tokens") or []
        reason_sim = _jaccard(cur_reason, old_reason)
        exact_reason = bool(features.get("reason_signature")) and features.get("reason_signature") == (cand.get("features") or {}).get("reason_signature")
        if not exact_reason and reason_sim < float(reason_min_jaccard):
            continue
        if not matched_fields and not exact_reason:
            continue

        score = (100.0 if exact_reason else 0.0) + (len(matched_fields) * 10.0) + reason_sim
        if score > best_score:
            best_score = score
            best = {
                "candidate": cand,
                "meta": {
                    "reason_jaccard": round(reason_sim, 4),
                    "reason_exact": bool(exact_reason),
                    "matched_fields": matched_fields,
                }
            }
    return best


def pick_history_examples(history_rows: List[Dict[str, Any]], pd01_text: str, pd02_text: str, reason: str, topk: int = 5) -> List[Dict[str, Any]]:
    src_toks = _norm_tokens(f"{pd01_text} {pd02_text} {reason}")
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for row in history_rows:
        v = str(row.get("vendor", ""))
        p = str(row.get("product", ""))
        r = str(row.get("reason", ""))
        c = str(row.get("category_ref_text", ""))
        if not c:
            continue
        toks = _norm_tokens(f"{v} {p} {r}")
        sim = _jaccard(src_toks, toks)
        if sim <= 0:
            continue
        scored.append((sim, {"vendor": v, "product": p, "reason": r, "category_ref_text": c, "similarity": round(sim, 4)}))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [x[1] for x in scored[:max(1, topk)]]


def ai_category_judge(pd01_text: str, pd02_text: str, reason: str, options: List[str], ai_cmd: Optional[str] = None, history_examples: Optional[List[Dict[str, Any]]] = None, allow_fallback: bool = False) -> CategoryJudgeResult:
    """
    If ai_cmd is provided, execute it and pass a JSON payload through STDIN.
    The command must return JSON with keys:
      category_ref_text, confidence, top3, rationale
    By default fallback is disabled and the function raises explicit errors
    so workflow can stop and surface a deterministic failure.
    """
    payload = {
        "task": "category_judge",
        "inputs": {
            "payment_detail_01_text": pd01_text,
            "payment_detail_02_text": pd02_text,
            "reason": reason,
            "options": options,
            "instruction": (
                "Choose exactly one category from options using JOINT semantics of PD01 + PD02 + Reason. "
                "Prefer semantic accounting fit over keyword overlap; distinguish consultancy/professional services "
                "from generic COS buckets when meaning supports it. "
                "Return strict JSON only: category_ref_text, confidence(0-1), top3, rationale."
            )
        }
    }

    if not ai_cmd:
        if allow_fallback:
            src = f"{pd01_text} | {pd02_text} | {reason}".lower()
            scored = []
            for opt in options:
                o = (opt or "").strip()
                ol = o.lower()
                if not o:
                    continue
                score = 1.0 if ol and ol in src else 0.0
                scored.append((score, o))
            scored.sort(key=lambda x: x[0], reverse=True)
            top3 = [x[1] for x in scored[:3]]
            best = top3[0] if top3 else ""
            conf = 0.6 if best else 0.2
            return CategoryJudgeResult(
                category_ref_text=best,
                confidence=conf,
                top3=top3,
                rationale="fallback heuristic (ai_cmd missing)",
            )
        raise RuntimeError("step2_ai_cmd_missing")

    try:
        p = subprocess.run(ai_cmd, input=json.dumps(payload, ensure_ascii=False), text=True, capture_output=True, shell=True)
    except Exception as e:
        if allow_fallback:
            src = f"{pd01_text} | {pd02_text} | {reason}".lower()
            scored = []
            for opt in options:
                o = (opt or "").strip()
                ol = o.lower()
                if not o:
                    continue
                score = 1.0 if ol and ol in src else 0.0
                scored.append((score, o))
            scored.sort(key=lambda x: x[0], reverse=True)
            top3 = [x[1] for x in scored[:3]]
            best = top3[0] if top3 else ""
            conf = 0.6 if best else 0.2
            return CategoryJudgeResult(
                category_ref_text=best,
                confidence=conf,
                top3=top3,
                rationale=f"fallback heuristic (ai_cmd exec error: {e})",
            )
        raise RuntimeError(f"step2_ai_cmd_exec_failed:{e}")

    if p.returncode != 0:
        if allow_fallback:
            src = f"{pd01_text} | {pd02_text} | {reason}".lower()
            scored = []
            for opt in options:
                o = (opt or "").strip()
                ol = o.lower()
                if not o:
                    continue
                score = 1.0 if ol and ol in src else 0.0
                scored.append((score, o))
            scored.sort(key=lambda x: x[0], reverse=True)
            top3 = [x[1] for x in scored[:3]]
            best = top3[0] if top3 else ""
            conf = 0.6 if best else 0.2
            return CategoryJudgeResult(
                category_ref_text=best,
                confidence=conf,
                top3=top3,
                rationale=f"fallback heuristic (ai_cmd nonzero: {p.returncode})",
            )
        raise RuntimeError(f"step2_ai_cmd_nonzero:{p.returncode}:{(p.stderr or '').strip()}")

    out = (p.stdout or '').strip()
    if not out:
        raise RuntimeError("step2_ai_cmd_empty_output")
    try:
        obj = json.loads(out)
    except Exception as e:
        raise RuntimeError(f"step2_ai_cmd_invalid_json:{e}")

    cat = str(obj.get("category_ref_text", "")).strip()
    if not cat:
        raise RuntimeError("step2_ai_cmd_missing_category_ref_text")

    return CategoryJudgeResult(
        category_ref_text=cat,
        confidence=float(obj.get("confidence", 0) or 0),
        top3=[str(x) for x in (obj.get("top3") or [])][:3],
        rationale=str(obj.get("rationale", "ai_cmd")),
    )


def parse_accounts(bill_rules_obj: Dict[str, Any]) -> List[str]:
    rules = bill_rules_obj.get("rules", bill_rules_obj)
    d = (rules.get("qboOptionDictionaries") or {})
    out = []
    for x in d.get("accounts", []) or []:
        if isinstance(x, dict):
            v = x.get("label") or x.get("key")
            if v:
                out.append(str(v))
        elif x:
            out.append(str(x))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parsed", required=True)
    ap.add_argument("--bill-rules", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--ai-cmd", help="shell command that reads JSON from stdin and returns strict JSON")
    ap.add_argument("--history", help="optional JSON file with confirmed mappings")
    ap.add_argument("--history-topk", type=int, default=5)
    ap.add_argument("--allow-fallback", choices=["true", "false"], default="false")
    ap.add_argument("--reuse-min-confidence", type=float, default=0.85)
    ap.add_argument("--reuse-reason-min-jaccard", type=float, default=0.7)
    args = ap.parse_args()

    parsed = json.loads(Path(args.parsed).read_text(encoding="utf-8"))
    bill_rules = json.loads(Path(args.bill_rules).read_text(encoding="utf-8"))
    accounts = parse_accounts(bill_rules)
    history_rows: List[Dict[str, Any]] = []
    if args.history and Path(args.history).exists():
        try:
            h = json.loads(Path(args.history).read_text(encoding="utf-8"))
            if isinstance(h, list):
                history_rows = h
            elif isinstance(h, dict):
                history_rows = h.get("records") or h.get("items") or []
        except Exception:
            history_rows = []

    records = parsed.get("records") or []
    if not records and parsed.get("recap"):
        records = [{"record_index": 0, "recap": parsed.get("recap", {}), "missing_required": parsed.get("missing_required", [])}]

    allow_fallback = str(args.allow_fallback).lower() == "true"

    started_at = time.perf_counter()
    out_records = []
    reuse_cache: List[Dict[str, Any]] = []
    per_record_metrics = []
    ai_calls = 0
    reused_records = 0
    ai_total_duration_ms = 0.0
    estimated_saved_ai_duration_ms = 0.0
    for r in records:
        record_started = time.perf_counter()
        recap = r.get("recap", {})
        pd01 = recap.get("payment_detail_01_text", "")
        pd02 = recap.get("payment_detail_02_text", "")
        reason = recap.get("reason", "")
        features = _build_reuse_features(pd01, pd02, reason)
        reuse = _find_reuse_candidate(features, reuse_cache, args.reuse_min_confidence, args.reuse_reason_min_jaccard)
        hist_count = 0
        if reuse:
            src = reuse["candidate"]
            base = src["result"]
            judged = CategoryJudgeResult(
                category_ref_text=base.category_ref_text,
                confidence=base.confidence,
                top3=list(base.top3),
                rationale=base.rationale,
            )
            judge_source = "batch_reuse"
            reused_from_record_index = src.get("record_index")
            reuse_meta = reuse.get("meta") or {}
            reused_records += 1
            estimated_saved_ai_duration_ms += float(src.get("duration_ms") or 0.0)
        else:
            hist = pick_history_examples(history_rows, pd01, pd02, reason, topk=max(1, args.history_topk)) if history_rows else []
            hist_count = len(hist)
            ai_started = time.perf_counter()
            judged = ai_category_judge(
                pd01,
                pd02,
                reason,
                accounts,
                ai_cmd=args.ai_cmd,
                history_examples=hist,
                allow_fallback=allow_fallback,
            )
            ai_duration_ms = round((time.perf_counter() - ai_started) * 1000.0, 2)
            ai_calls += 1
            ai_total_duration_ms += ai_duration_ms
            judge_source = "ai"
            reused_from_record_index = None
            reuse_meta = None
            if float(judged.confidence or 0) >= float(args.reuse_min_confidence):
                reuse_cache.append({
                    "record_index": r.get("record_index"),
                    "features": features,
                    "result": judged,
                    "duration_ms": ai_duration_ms,
                })

        rr = dict(r)
        rr["category_ai"] = asdict(judged)
        rr["category_ai"]["history_examples_used"] = hist_count
        rr["category_ai"]["judge_source"] = judge_source
        rr["category_ai"]["reused_from_record_index"] = reused_from_record_index
        rr["category_ai"]["reuse_meta"] = reuse_meta
        rr["category_ai"]["duration_ms"] = round((time.perf_counter() - record_started) * 1000.0, 2)
        out_records.append(rr)
        per_record_metrics.append({
            "record_index": r.get("record_index"),
            "duration_ms": rr["category_ai"]["duration_ms"],
            "judge_source": judge_source,
            "reused_from_record_index": reused_from_record_index,
        })

    out = {
        "ok": True,
        "source": "step2_match.py",
        "records": out_records,
        "accounts_count": len(accounts),
        "history_records": len(history_rows),
        "metrics": {
            "records_total": len(records),
            "ai_calls": ai_calls,
            "reused_records": reused_records,
            "reuse_cache_size": len(reuse_cache),
            "reuse_min_confidence": float(args.reuse_min_confidence),
            "reuse_reason_min_jaccard": float(args.reuse_reason_min_jaccard),
            "category_stage_duration_ms": round((time.perf_counter() - started_at) * 1000.0, 2),
            "ai_total_duration_ms": round(ai_total_duration_ms, 2),
            "ai_avg_duration_ms": round(ai_total_duration_ms / max(1, ai_calls), 2) if ai_calls else 0.0,
            "record_avg_duration_ms": round(sum(x["duration_ms"] for x in per_record_metrics) / max(1, len(per_record_metrics)), 2) if per_record_metrics else 0.0,
            "estimated_saved_ai_duration_ms": round(estimated_saved_ai_duration_ms, 2),
            "slowest_records": sorted(per_record_metrics, key=lambda x: x["duration_ms"], reverse=True)[:3],
        },
        "notes": "AI judge uses PD01+PD02+Reason joint semantics with optional history_examples context"
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "out": str(Path(args.out).resolve()), "records": len(out_records)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
