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
