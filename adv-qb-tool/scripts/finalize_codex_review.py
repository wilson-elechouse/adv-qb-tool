#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from workflow import publish_output_artifacts, read_json, run, script_path, write_json


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True, help="workflow_state.json from a WAIT_CODEX_REVIEW run")
    ap.add_argument("--decisions", required=True, help="Codex review decisions JSON")
    ap.add_argument("--judge-source", default="codex_review")
    ap.add_argument("--provider", default="codex_review")
    args = ap.parse_args()

    state_path = Path(args.state).resolve()
    decisions_path = Path(args.decisions).resolve()
    if not state_path.exists():
        raise RuntimeError(f"state_file_missing:{state_path}")
    if not decisions_path.exists():
        raise RuntimeError(f"decisions_file_missing:{decisions_path}")

    state = read_json(state_path)
    workdir = state_path.parent.resolve()
    artifacts = state.get("artifacts") or {}
    inputs = state.get("inputs") or {}
    step2_path = Path(str(artifacts.get("step2_ai_judge") or "")).resolve()
    summary_path = Path(str(artifacts.get("batch_match_summary") or "")).resolve()
    bill_rules = Path(str(inputs.get("bill_rules") or "")).resolve()
    recap_path = workdir / "confirmation_recap.json"
    review_mode = str(inputs.get("codex_review_mode") or "review-worthy")

    if not step2_path.exists():
        raise RuntimeError(f"step2_missing:{step2_path}")
    if not summary_path.exists():
        raise RuntimeError(f"summary_missing:{summary_path}")
    if not bill_rules.exists():
        raise RuntimeError(f"bill_rules_missing:{bill_rules}")

    run([
        "python", str(script_path("apply_codex_review_decisions.py")),
        "--step2", str(step2_path),
        "--decisions", str(decisions_path),
        "--bill-rules", str(bill_rules),
        "--judge-source", str(args.judge_source),
        "--provider", str(args.provider),
    ])

    run([
        "python", str(script_path("merge_step2_into_batch.py")),
        "--step2", str(step2_path),
        "--summary", str(summary_path),
    ])

    queue_path = workdir / "codex_review_queue.json"
    run([
        "python", str(script_path("build_codex_review_queue.py")),
        "--step2", str(step2_path),
        "--summary", str(summary_path),
        "--out", str(queue_path),
        "--mode", review_mode,
    ])

    queue_obj = read_json(queue_path)
    state.setdefault("artifacts", {})["codex_review_queue_file"] = str(queue_path.resolve())
    state["artifacts"]["codex_review_decisions_file"] = str(decisions_path.resolve())
    state.setdefault("metrics", {})["codex_review_queue"] = queue_obj.get("counts") or {}

    remaining = int(((queue_obj.get("counts") or {}).get("queued_records")) or 0)
    if remaining > 0:
        state["state"] = "WAIT_CODEX_REVIEW"
        write_json(state_path, state)
        print(
            json.dumps(
                {
                    "ok": True,
                    "state": state["state"],
                    "state_file": str(state_path),
                    "codex_review_queue_file": str(queue_path.resolve()),
                    "codex_review_queue_count": remaining,
                    "note": "codex_review_still_pending",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    run([
        "python", str(script_path("step3_render_recap.py")),
        "--summary", str(summary_path),
        "--out", str(recap_path),
    ])

    summary_obj = read_json(summary_path)
    if summary_obj.get("canonical_preview_file"):
        state["artifacts"]["canonical_preview_file"] = str(Path(summary_obj["canonical_preview_file"]).resolve())
    if summary_obj.get("canonical_ready_file"):
        state["artifacts"]["canonical_ready_file"] = str(Path(summary_obj["canonical_ready_file"]).resolve())
    if summary_obj.get("vendor_ai_audit_file"):
        state["artifacts"]["vendor_ai_audit_file"] = str(Path(summary_obj["vendor_ai_audit_file"]).resolve())
    if summary_obj.get("account_ai_review_log_file"):
        state["artifacts"]["account_ai_review_log_file"] = str(Path(summary_obj["account_ai_review_log_file"]).resolve())
    if summary_obj.get("account_ai_review_summary_file"):
        state["artifacts"]["account_ai_review_summary_file"] = str(Path(summary_obj["account_ai_review_summary_file"]).resolve())
    state["artifacts"]["confirmation_recap"] = str(recap_path.resolve())
    state["state"] = "WAIT_CONFIRMATION"
    write_json(state_path, state)

    published = publish_output_artifacts(workdir, state_path, recap_path, summary_obj)
    state["artifacts"]["published_result_file"] = published["result_file"]
    state["artifacts"]["issues_report_file"] = published["local_issues_file"]
    state["artifacts"]["readable_issues_report_file"] = published["local_readable_issues_file"]
    state["artifacts"]["local_ready_file"] = published["local_result_file"]
    state["artifacts"]["published_issues_file"] = published["issues_file"]
    state["artifacts"]["published_readable_issues_file"] = published["issues_readable_file"]
    state["artifacts"]["published_ai_log_file"] = published["ai_log_file"]
    state["artifacts"]["published_ai_summary_file"] = published["ai_summary_file"]
    state["artifacts"]["published_step2_ai_judge_file"] = published["step2_ai_judge_file"]
    state["artifacts"]["published_codex_review_queue_file"] = published.get("codex_review_queue_file", "")
    if published.get("vendor_ai_audit_file"):
        state["artifacts"]["published_vendor_ai_audit_file"] = published["vendor_ai_audit_file"]
    state["artifacts"]["published_ai_logs_manifest"] = published["ai_logs_manifest"]
    write_json(state_path, state)

    print(
        json.dumps(
            {
                "ok": True,
                "state": state["state"],
                "state_file": str(state_path),
                "recap": str(recap_path.resolve()),
                "published_result_file": published["result_file"],
                "published_issues_file": published["issues_file"],
                "published_readable_issues_file": published["issues_readable_file"],
                "published_ai_log_file": published["ai_log_file"],
                "published_ai_summary_file": published["ai_summary_file"],
                "published_codex_review_queue_file": published.get("codex_review_queue_file", ""),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
