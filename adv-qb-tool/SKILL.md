---
name: adv-qb-tool
description: Run and troubleshoot the ADV QB payment-request workflow bundle for Excel inputs, bill-rule matching, chunked processing, and recap output. Use when Codex needs to execute or inspect the bundled scripts, validate AI runtime setup, run a single workflow against an xlsx file, start or resume a chunked batch job, retry failed chunks, or inspect outputs under output/results and output/ai_logs.
---

# ADV QB Tool

## Overview

Use the bundled Python scripts to transform a payment-request workbook into canonical bill outputs. Prefer the Codex entrypoints `scripts/workflow_codex.py` and `scripts/start_chunk_job_codex.py`.

When using this skill in a Codex session, do not stop at `WAIT_CODEX_REVIEW` by default. Treat it as an internal checkpoint: inspect `codex_review_queue.json`, prepare decisions in-session, run `finalize_codex_review.py`, and only then report final outputs unless the user explicitly asks to pause before review.

When a run produces failed or user-action bills, always generate and prefer the readable Markdown issue report over raw JSON in user-facing communication. Use `canonical_bills.issues.json` as the machine-readable source, but summarize or point the user to `issues_readable_report.<timestamp>.<run>.md` unless the user explicitly asks for raw data.

## Quick Start

1. Work from this skill directory when invoking `run.cmd`, `check_env.cmd`, or the Python scripts so relative paths resolve correctly.
2. Verify the runtime with `check_env.cmd`. The default Codex profile now enforces a final Codex review gate before confirmation; external AI is optional.
3. Use `references/config/bill-rules.xnofi.json` as the default rules file unless the user supplies a newer snapshot.
4. Use `references/config/field-mapping.xnofi.toml` for hand-maintained field mapping. JSON is still supported for backward compatibility.

If the external bridge is meant for one-shot batch review, verify it with:

```bash
python scripts/check_ai_runtime.py --profile codex --category-ai-mode batch-review
```

## Single Run

Run:

```bash
python scripts/workflow_codex.py --file <xlsx> --bill-rules references/config/bill-rules.xnofi.json --dir tmp/run
```

`workflow_codex.py` now defaults to `--codex-review-mode all-eligible --require-codex-review`, so every run enters a Codex review gate before final confirmation. In this strict mode the workflow also skips auto-chunking, so one workbook yields one merged review queue. In Codex sessions, the expected behavior is to continue through that gate automatically and finalize the run unless the user asks to inspect the queue first. If the workbook is already parsed, pass `--parsed <parse_result.json>` to skip the parsing step.

For one-shot batch AI review after the code-selected account is built, use:

```bash
python scripts/workflow_codex.py --file <xlsx> --bill-rules references/config/bill-rules.xnofi.json --dir tmp/run --ai-cmd "<external-ai-cmd>" --category-ai-mode batch-review --batch-ai-review-mode all-eligible
```

This path first builds the program result, then packages the raw source fields plus the current account choice into one controlled JSON payload and calls `ai_cmd` once for the selected records.

For strict Codex participation on every record, use:

```bash
python scripts/workflow_codex.py --file <xlsx> --bill-rules references/config/bill-rules.xnofi.json --dir tmp/run --codex-review-mode all-eligible --require-codex-review
```

This pauses the run in `WAIT_CODEX_REVIEW` until Codex review decisions are applied.

## Chunked Run

Start:

```bash
python scripts/start_chunk_job_codex.py --file <xlsx> --bill-rules references/config/bill-rules.xnofi.json --chunk-size 10 --max-batches-per-run 1
```

Then use:

- `python scripts/resume_chunk_job.py`
- `python scripts/chunk_job_status.py`
- `python scripts/retry_failed_chunk_job.py`

Use chunking for larger files or when the user wants resumable processing.

## AI Runtime

External AI is optional in the Codex profile. Use `--ai-cmd`, `CODEX_AI_CMD`, or `ADV_QB_AI_CMD` to add a real AI reviewer for category selection; otherwise the run uses the built-in fallback logic and then pauses for Codex review.

With the default Codex wrappers, every run is expected to end with a Codex review gate. The wrapper enforces `--codex-review-mode all-eligible --require-codex-review`, so nothing reaches final confirmation until Codex decisions are applied. In that mode, auto-chunking is intentionally skipped to keep review in one queue.

## Codex Review Flow

`workflow_codex.py` writes `codex_review_queue.json` into the run workdir and publishes a copy under `output/ai_logs/intermediate/`. With the default Codex wrappers, the run enters `WAIT_CODEX_REVIEW` and expects this queue to be reviewed before confirmation.

Typical flow:

1. Run the normal workflow.
2. Review `codex_review_queue.json` in this Codex session.
3. Save decisions as JSON and apply them:

```bash
python scripts/apply_codex_review_decisions.py --step2 <workdir/step2_ai_judge.json> --decisions <decisions.json> --bill-rules references/config/bill-rules.xnofi.json
```

4. Rebuild batch outputs:

```bash
python scripts/merge_step2_into_batch.py --step2 <workdir/step2_ai_judge.json> --summary <workdir/batch/batch_match_summary.json>
python scripts/step3_render_recap.py --summary <workdir/batch/batch_match_summary.json> --out <workdir/confirmation_recap.json>
```

The default Codex wrappers already use `all-eligible`, so every valid record is queued for Codex review unless you override the mode.

Default skill behavior:

1. Run `workflow_codex.py`.
2. If the result is `WAIT_CODEX_REVIEW`, continue in-session instead of stopping.
3. Review the queue, write decisions, run `finalize_codex_review.py`, and only then report final outputs to the user.
4. Pause only when the user explicitly asks to review the queue manually, or when Codex cannot make a defensible decision on one or more records.

To finalize a strict review run after Codex has prepared decisions:

```bash
python scripts/finalize_codex_review.py --state <workdir/workflow_state.json> --decisions <decisions.json>
```

In strict mode, a run is not final until `finalize_codex_review.py` completes and the queue count reaches 0.

## Outputs

Expect:

- `output/results/canonical_bills.ready.json`
- `output/results/canonical_bills.issues.json`
- `output/ai_logs/issues_readable_report.<timestamp>.<run>.md`
- `output/ai_logs/*.jsonl`
- `output/ai_logs/intermediate/*.json`

In chunked runs, both result files are merged across all completed batches before publishing. `canonical_bills.ready.json` contains the combined ready bills; `canonical_bills.issues.json` contains failed and user-action items in one consolidated report. A readable Markdown issue report is also published under `output/ai_logs/`.

For issue handling, treat the readable Markdown report as the default review artifact:

1. Generate it whenever `canonical_bills.issues.json` is non-empty.
2. Prefer quoting or summarizing this Markdown report when answering the user.
3. Only fall back to raw JSON fields when the user asks for source data, debugging details, or exact machine-readable payloads.

## Configuration Notes

Use `references/config/category-mapping.xnofi.json` and `references/config/bill-rules.xnofi.json` as bundled defaults. Avoid editing bundled rule files unless the user explicitly asks. Prefer snapshots or override paths.

Use `references/config/field-mapping.xnofi.toml` to control:

- `[excel_columns]`: map Excel column titles to the parser's internal source fields.
- `[outputs.<name>].relation = "equals"`: direct source-to-output matching, such as `Location -> location_ref_text`.
- `[outputs.<name>].relation = "associated"`: combine multiple source fields as evidence for a target field, such as `Payment Details 02 + Reasons for payment -> account_ref_text`.

`source_fields` / `sourceFields` refer to internal recap fields, not only Excel headers. For example, `payment_type` prefers the `Payment Type` column and falls back to `Payment Details 01 -> Payment Type`, while `product` is derived from `Payment Details 02 -> Product`.

`category_ref_text` inherits the `account_ref_text` mapping by default. Only add an explicit `category` / `category_ref_text` block when category truly needs different source fields.

The loader also accepts the legacy JSON shape with `headerAliases` and `outputFieldMappings`, but TOML is the default because comments and grouped sections are easier to maintain by hand.

## Debugging

If a command fails, first re-run `python scripts/check_ai_runtime.py --profile codex`. Use `--help` on the entrypoint script before changing CLI arguments. Inspect output JSON and AI logs before modifying matching logic.
