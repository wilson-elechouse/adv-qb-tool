# Quickstart Config (V3)

Create a config JSON and pass with `--config`.

```json
{
  "base_url": "https://qb.uudc.us",
  "tenant_id": "t-adv",
  "username": "admin",
  "password": "admin"
}
```

Notes:
- `base_url`: Collector V3 web host.
- `tenant_id`: target QBO company/tenant.
- `username/password`: used by scripts to login and establish session cookies.
- Current policy: scripts run create + validate + precheck only; user final submit is manual in Collector UI.
- Vendor mode is pending-only: `submit_vendor.mjs` only pushes vendor candidates to Collector rule dictionary; user must manually create/edit vendor in Collector Vendor Management.
- Missing/ambiguous data must be confirmed with user first; do not auto-fill defaults.
- Missing mapping rules (vendor/account/tax/class/date/source-column mapping) must be confirmed by user before creating submissions.
- Conversation default: do not ask user to choose tenant/company; use the current Collector account's bound QBO company unless user explicitly requests override.
- File classification default: if uploaded document indicates `Payment Request`, default operation kind to `Bill` (not `BillPayment`) unless user explicitly overrides.
- Confirmation hard gate: before any create/validate/precheck execution, the agent must present bill-value recap (vendor/bill no/request no/date/location/account/class/withholding tax/description/memo) and wait for explicit user confirmation (`确认执行` / `继续` / `yes` / `ok go`).
- Do not use command lines, payload paths, or sample-file names as the default confirmation content shown to end users.
- For uploaded Excel files, use combined parse+identify first (`python scripts/parse_payment_request_xlsx.py --file <uploaded.xlsx> --out <parse_result.json>`), then recap extracted business values before asking confirmation.
- Identification rule: if `Billing/SOA NO.` + `Billing End Date` + `Billing Start Date` are all present, classify as Bill.
- If the three-field rule is not met, ask user to specify Bill or BillPayment before mapping.
- Approved-only filter: only records with `status=approved` participate in extraction/mapping/upload preparation.
- Bill Rule pull policy: do not live-pull every run; use existing dictionary first. Trigger live pull only for unmatched/low-confidence fields, then retry mapping.
- Manual refresh command (recommended before important runs):
  - `node skills/adv-qbo-tool/scripts/refresh_bill_rules.mjs --config references/companies/example.json --out-dir tmp/adv-qbo/rules_cache --set-latest true`
  - Output includes: `changed`, `snapshot`, `latest`, `manifest`, `hash`.
- Step-3 fixed output build (single record):
  - `node scripts/build_match_result.mjs --parsed <parse_result.json> --bill-rules <bill-rules.json> --out <match_result.json>`
- Step-3 fixed output build (multi-row with chunking):
  - `node scripts/build_match_batch.mjs --parsed <parse_result.json> --bill-rules <bill-rules.json> --outDir <batch_dir> --chunk-size 10`
  - If rows > 10, records are processed in loops of 10 until complete.
  - Optional: pass `--user-decisions <user_decisions.json>` for multi-round confirmations.
  - Optional: pass `--continue-on-unresolved true` to apply blank-by-policy and produce `ready_with_blanks` when user insists to continue.
  - Optional: pass `--confidence-config references/config/confidence-weights.bill.json` to use configurable confidence formula/weights.
- Confirmation recap rendering (hard-gated):
  - `node scripts/render_confirmation_recap.mjs --match-result <match_result.json> --state-file <workflow_state.json> --via-workflow true`
  - Renderer verifies workflow proof (`state-file`) and blocks mismatched/non-workflow recap outputs.
  - If unresolved fields have no alternatives and bill-rules are available, renderer exits with `suggestion_render_incomplete`.
- Deterministic workflow state machine (only entry):
  - `python skills/adv-qbo-tool/scripts/workflow.py --file <uploaded.xlsx> --bill-rules <bill_rules.json> --dir <workdir> --chunk-size 10`
  - For chat-safe start, prefer:
    - `python skills/adv-qbo-tool/scripts/start_chunk_job.py --file <uploaded.xlsx> --bill-rules <bill_rules.json> --chunk-size 10 --max-batches-per-run 1`
  - For large Excel files, prefer bounded runs:
    - `python skills/adv-qbo-tool/scripts/start_chunk_job.py --file <uploaded.xlsx> --bill-rules <bill_rules.json> --chunk-size 10 --max-batches-per-run 1`
    - Continue same job:
      - `python skills/adv-qbo-tool/scripts/resume_chunk_job.py`
    - Or resume the most recent waiting chunk job:
      - `python skills/adv-qbo-tool/scripts/resume_chunk_job.py`
  - Optional manual snapshot priority: `--manual-rules-snapshot <rules_cache/bill-rules.YYYYMMDD-HHMMSS.json>`
  - Optional TTL refresh control: `--rules-ttl-seconds 21600` (default 6h)
  - Rules priority in workflow: `manual_snapshot > rules_cache/latest.json > input --bill-rules`
  - Default AI bridge is built-in (`scripts/ai_bridge.py`) via `references/config/ai-runtime.json`.
  - Uses fixed states: `S1_PARSE_IDENTIFY -> S2_MATCH_BUILD -> S3_CONFIRM_RENDER -> S4_UPLOAD -> S5_DONE`.
  - If not confirmed, exits at `WAIT_CONFIRMATION` with `confirmation_recap.json`.
  - If rows exceed `--chunk-size`, workflow auto-splits into batch workdirs and may exit at `WAIT_NEXT_BATCH`.
  - Chunk job progress files:
    - `workflow_state.json`
    - `chunk_job_summary.json`
    - `chunk_job_report.json`
    - `python skills/adv-qbo-tool/scripts/chunk_job_status.py`
    - Retry failed records only:
      - `python skills/adv-qbo-tool/scripts/retry_failed_chunk_job.py`
  - `node scripts/run_workflow.mjs` is blocked and must not be used.
  - Direct calls to `build_match_result` / `build_match_batch` / `render_confirmation_recap` / `submit_from_match_result` are locked unless `--via-workflow true` is provided by the orchestrator.
- Step-4 upload (single-source path):
  - `node scripts/submit_from_match_result.mjs --config references/companies/example.json --match-result <match_result.json> --confirmation-received true`
  - The script enforces `status in [ready, ready_with_blanks]` and consumes `collector_payload.draft` only.
- Legacy direct submit scripts are disabled by default and require explicit override:
  - `--allow-legacy-direct true`
- Guardrail: if xlsx exists but parse output is missing/empty, stop with `parse_failed_or_empty`; do not output generic recap template and do not execute create/validate/precheck.
- Vendor unmatched handling: ask user once for vendor confirmation; if still unresolved, allow empty vendor upload (`vendor_ref_text=''`) and require user to edit vendor on Collector UI.
- Bill field mapping baseline (Wilson 2026-03-02):
  - `bill_no <- Billing/SOA NO.`
  - `client_ref <- Request No.` (Collector unique business id)
  - `location_ref_text <- source.location`; if missing/empty, fallback to Collector rule default location
  - `line.class_ref_text <- Belongs To` (nullable)
  - `line.description <- Feishu/Billing Date/Period Covered/Business Unit(Belongs To)/Reason`
  - bill_date empty policy: warn first; if user insists, keep empty in Collector
  - memo policy: include AI tag + operator account id (e.g., Telegram ID)
  - after QBO success, ensure `bill_id` is written back so `request_no <-> bill_id` is traceable
- For batch, use `scripts/batch_run.mjs` with `references/samples/batch.sample.json`, and pass `--mapping-confirmed true` only after user confirms mapping.
- XNOFI mapping baseline (2026-02-21) is persisted at `references/config/category-mapping.xnofi.json`.
- Confirmation stop-conditions are persisted at `references/config/confirmation-guards.xnofi.json`.
- Standard audit outputs should be kept as:
  - `references/samples/mapping_preview.latest.json`
  - `references/samples/precheck_report.latest.json`
  - `references/samples/confirmation_questions.latest.json`
