#!/usr/bin/env python3
import json
import re
import sys
from typing import List, Tuple


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def score_option(opt: str, pd01: str, pd02: str, reason: str) -> float:
    o = norm(opt)
    text = norm(f"{pd01} {pd02} {reason}")

    score = 0.0

    # Strong consultancy signals
    consultancy_hits = [
        "consult", "consultancy", "consultant", "professional service", "management fee", "service-consultancy"
    ]
    if any(k in text for k in consultancy_hits):
        if "5702" in o or "consult" in o or "professional" in o:
            score += 0.75

    # Gasoline/fuel signals
    fuel_hits = ["gasoline", "fuel", "diesel", "petrol"]
    if any(k in text for k in fuel_hits):
        if "gasoline" in o or "fuel" in o:
            score += 0.7

    # Warehouse supplies signals
    wh_hits = ["warehouse", "supplies", "consumables", "inventory"]
    if any(k in text for k in wh_hits):
        if "warehouse" in o or "supplies" in o:
            score += 0.65

    # Employee-related signals
    emp_hits = ["employee", "staff", "hr", "benefit", "allowance", "salary", "payroll"]
    if any(k in text for k in emp_hits):
        if "employee" in o or "salary" in o or "payroll" in o:
            score += 0.65

    # Generic lexical overlap fallback
    otoks = set(re.findall(r"[a-z0-9]+", o))
    ttoks = set(re.findall(r"[a-z0-9]+", text))
    if otoks and ttoks:
        inter = len(otoks & ttoks)
        score += min(0.35, inter / max(1, len(otoks)) * 0.35)

    return min(1.0, score)


def main():
    raw = sys.stdin.read()
    if not raw.strip():
        raise RuntimeError("empty_input")
    obj = json.loads(raw)
    inputs = (obj or {}).get("inputs", {})
    pd01 = inputs.get("payment_detail_01_text", "")
    pd02 = inputs.get("payment_detail_02_text", "")
    reason = inputs.get("reason", "")
    options: List[str] = inputs.get("options", []) or []

    if not options:
        print(json.dumps({
            "category_ref_text": "",
            "confidence": 0.0,
            "top3": [],
            "rationale": "no_options"
        }, ensure_ascii=False))
        return

    scored: List[Tuple[float, str]] = []
    for opt in options:
        if not str(opt).strip():
            continue
        scored.append((score_option(str(opt), pd01, pd02, reason), str(opt)))

    scored.sort(key=lambda x: x[0], reverse=True)
    top3 = [x[1] for x in scored[:3]]
    best_score, best = scored[0] if scored else (0.0, "")

    rationale = "semantic_rule_judge: PD01+PD02+Reason"
    out = {
        "category_ref_text": best,
        "confidence": round(float(best_score), 4),
        "top3": top3,
        "rationale": rationale,
    }
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
