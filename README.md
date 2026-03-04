# adv-qbo-tool

`adv-qbo-tool` is an OpenClaw skill for building and validating QuickBooks Bill and BillPayment drafts against Collector V3.

## Repo layout
- `SKILL.md`: skill policy and workflow contract
- `scripts/`: parser, matcher, recap, submit, and workflow entrypoints
- `references/`: config, examples, and mapping references
- `tmp/`: local runtime output only, excluded from Git

## Main workflow
Use the Python workflow entrypoint:

```bash
python scripts/workflow.py --file <uploaded.xlsx> --bill-rules <bill_rules.json> --dir <workdir>
```

For large Excel files, keep each invocation bounded and resumable:
```bash
python scripts/start_chunk_job.py --file <uploaded.xlsx> --bill-rules <bill_rules.json> --chunk-size 10 --max-batches-per-run 1 --auto-continue-seconds 10
python scripts/resume_chunk_job.py
python scripts/retry_failed_chunk_job.py
```

Default large-file behavior:
- process 10 rows per batch
- if a batch has no unresolved items and no failures, auto-continue after 10 seconds
- if a batch has unresolved items or failures, pause and wait for user input

## Local config
For Collector connectivity, copy and adapt:

```json
{
  "base_url": "https://qb.uudc.us",
  "tenant_id": "t-adv",
  "username": "admin",
  "password": "admin"
}
```

Start from `references/companies/example.json` and keep private variants out of Git.

## Notes
- Category selection uses a dedicated OpenClaw agent bridge via `scripts/ai_bridge.py`
- Final submit to QBO is manual in Collector UI
- Runtime artifacts under `tmp/` should not be committed
