#!/usr/bin/env python3
import argparse
import copy
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from chunk_job_runtime import (
    read_json,
    write_json,
    resolve_default_search_root,
    launch_background,
)


def pick_latest_failed_job(root: Path):
    candidates = []
    for report_path in root.rglob("chunk_job_report.json"):
        try:
            report = read_json(report_path)
        except Exception:
            continue
        if int(report.get("failed_count", 0)) <= 0:
            continue
        candidates.append((report_path.stat().st_mtime, report_path, report))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x[0], reverse=True)
    _, report_path, report = candidates[0]
    return report_path.parent, report


def build_retry_parse(parsed, selected_records, source_parse_path: Path, source_workdir: Path):
    out = copy.deepcopy(parsed)
    first = (selected_records or [{}])[0] or {}
    out["records"] = copy.deepcopy(selected_records)
    out["recap"] = copy.deepcopy(first.get("recap") or parsed.get("recap") or {})
    out["missing_required"] = copy.deepcopy(first.get("missing_required") or out.get("missing_required") or [])
    rows = copy.deepcopy(out.get("rows") or {})
    rows["approved_in_retry"] = len(selected_records)
    rows["retry_size"] = len(selected_records)
    out["rows"] = rows
    out["retry"] = {
        "source_workdir": str(source_workdir.resolve()),
        "source_parse_result": str(source_parse_path.resolve()),
        "reason": "retry_failed_records",
        "failed_record_indexes": [r.get("record_index") for r in selected_records],
    }
    return out


def next_retry_dir(workdir: Path):
    retry_root = workdir / "retries"
    retry_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return retry_root / f"retry-{stamp}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", help="explicit chunk-job workdir")
    ap.add_argument("--root", default=None, help="search root when --workdir is omitted; defaults to workspace tmp/adv-qbo")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--wait", action="store_true", help="wait for the retry run to finish instead of starting in background")
    args = ap.parse_args()

    if args.workdir:
        workdir = Path(args.workdir).resolve()
        report_path = workdir / "chunk_job_report.json"
        if not report_path.exists():
            raise RuntimeError(f"chunk_job_report_missing:{report_path}")
        report = read_json(report_path)
    else:
        primary_root, roots = resolve_default_search_root(args.root)
        workdir, report = None, None
        root = primary_root
        for root in roots:
            workdir, report = pick_latest_failed_job(root)
            if workdir is not None:
                break
        if workdir is None:
            raise RuntimeError(f"no_failed_chunk_job_found_under:{root}")

    state_path = workdir / "workflow_state.json"
    if not state_path.exists():
        raise RuntimeError(f"workflow_state_missing:{state_path}")
    state = read_json(state_path)
    inputs = state.get("inputs") or {}
    flags = state.get("flags") or {}

    failed_indexes = []
    for item in report.get("failed_items", []):
        try:
            failed_indexes.append(int(item.get("record_index")))
        except Exception:
            continue
    failed_indexes = sorted(set(failed_indexes))
    if not failed_indexes:
        raise RuntimeError("no_failed_record_indexes_found")

    parse_path = Path((state.get("artifacts") or {}).get("parse_result") or "")
    if not parse_path.exists():
        raise RuntimeError(f"parse_result_missing:{parse_path}")
    parsed = read_json(parse_path)
    lookup = {}
    for record in parsed.get("records") or []:
        try:
            lookup[int(record.get("record_index", 0))] = record
        except Exception:
            continue
    selected_records = [lookup[idx] for idx in failed_indexes if idx in lookup]
    if not selected_records:
        raise RuntimeError("failed_records_not_found_in_parse_result")

    retry_dir = next_retry_dir(workdir)
    retry_parse_path = retry_dir / "parse_result.retry_failed.json"
    retry_parse = build_retry_parse(parsed, selected_records, parse_path, workdir)
    write_json(retry_parse_path, retry_parse)

    chunk_size = inputs.get("chunk_size") or 10
    max_batches = inputs.get("max_batches_per_run") or 1
    cmd = [
        sys.executable,
        str((Path(__file__).resolve().parent / "workflow.py").resolve()),
        "--parsed", str(retry_parse_path.resolve()),
        "--bill-rules", str(Path(inputs["bill_rules"]).resolve()),
        "--dir", str(retry_dir.resolve()),
        "--chunk-size", str(chunk_size),
        "--max-batches-per-run", str(max_batches),
        "--ai-runtime-config", str(inputs.get("ai_runtime_config") or "skills/adv-qbo-tool/references/config/ai-runtime.json"),
    ]
    if inputs.get("config"):
        cmd += ["--config", str(Path(inputs["config"]).resolve())]
    if inputs.get("history"):
        cmd += ["--history", str(inputs["history"])]
    if inputs.get("manual_rules_snapshot"):
        cmd += ["--manual-rules-snapshot", str(Path(inputs["manual_rules_snapshot"]).resolve())]
    if inputs.get("rules_cache_dir"):
        cmd += ["--rules-cache-dir", str(Path(inputs["rules_cache_dir"]).resolve())]
    if inputs.get("ai_cmd"):
        cmd += ["--ai-cmd", str(inputs["ai_cmd"])]
    if bool(flags.get("confirmed")):
        cmd.append("--confirmed")

    if args.dry_run:
        import json
        print(json.dumps({
            "ok": True,
            "mode": "retry_failed_chunk_job",
            "source_workdir": str(workdir.resolve()),
            "retry_workdir": str(retry_dir.resolve()),
            "failed_record_indexes": failed_indexes,
            "retry_parse": str(retry_parse_path.resolve()),
            "command": cmd,
        }, ensure_ascii=False, indent=2))
        return

    if args.wait:
        p = subprocess.run(cmd, text=True, capture_output=True)
        if p.returncode != 0:
            raise RuntimeError((p.stderr or p.stdout or "retry_failed_chunk_job_failed").strip())
        print((p.stdout or "").strip())
        return

    launch = launch_background(cmd, retry_dir, mode="retry-failed")
    import json
    print(json.dumps({
        "ok": True,
        "mode": "retry_failed_chunk_job",
        "source_workdir": str(workdir.resolve()),
        "retry_workdir": str(retry_dir.resolve()),
        "failed_record_indexes": failed_indexes,
        "retry_parse": str(retry_parse_path.resolve()),
        "next_action": "check_status_later",
        **launch,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
