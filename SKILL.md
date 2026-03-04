---
name: adv-qbo-tool
description: Build and validate QuickBooks Bill and BillPayment on Collector V3 with guided confirmation, validate/precheck flow, and traceable submission/result links. Use when an AI agent needs reusable V3 operation flow, tenant-aware submission handling, and deterministic payload submission for bill or bill_payment before manual user final submit.
---

# Adv QBO Tool (Collector V3)

Use this skill to run Bill / BillPayment on V3 safely.

## Default conversation flow
1. Parse + identify in one step from uploaded content.
2. Classification rule: if all three fields exist (`Billing/SOA NO.`, `Billing End Date`, `Billing Start Date`), classify as `Bill`.
3. If the three-field rule is not satisfied, AI asks user to specify transaction type (`Bill` or `BillPayment`) before proceeding.
4. Do not ask tenant/company in normal flow; use the Collector account's bound QBO company by default.
5. Collect required business fields first (never ask for script args/file paths in normal user flow).
6. Match using local/current Bill Rule dictionaries first (Vendor/Category/Location/Tax).
7. If list matching fails or confidence is low, run Bill Rule live refresh as an error-handling retry, then re-match.
8. Show full business recap (bill values user can verify).
9. Ask for explicit user confirmation and pause.
10. Only after explicit confirmation, create draft on V3.
11. Run validate + precheck and explain result.
12. Return result URL and ask user to login platform for manual review + final submit.

## Important policy
- This skill does **not** call final submit to QBO.
- Final submit must be done manually by user on Collector UI.
- Vendor flow is **pending-only**: agent only pushes vendor candidates into Collector rule dictionary for review; user creates/edits vendor manually in Vendor Management.
- Every draft/candidate created by this skill is tagged with: `[AI_AGENT][adv-qbo-tool]` for platform filtering.

## User-confirmation-first policy (strict)
- If required data is missing/invalid/ambiguous, **ask the user first**; do not auto-fill with defaults.
- If mapping is missing (vendor/account/tax/class/field-to-field mapping), **ask user to confirm mapping rules** before creating any submission.
- Do **not** silently auto-correct business fields (e.g., bill_date/due_date relationship). Instead:
  1) report the issue clearly,
  2) propose options,
  3) wait for explicit user confirmation,
  4) then execute.
- For uploaded files with new/unfamiliar schema, produce a "mapping confirmation checklist" and pause until user confirms.
- Company selection is skipped by default; ask tenant/company only when user explicitly requests an override.
- Classification rule for uploaded files: if document type/title indicates **Payment Request**, treat it as `Bill` by default (not `BillPayment`) unless user explicitly overrides.
- **Hard gate:** before any `create/validate/precheck` call, require explicit confirmation from user (accepted tokens: `确认` / `确认执行` / `继续` / `yes` / `ok go`).
- If explicit confirmation is absent, return recap + confirmation prompt only; do not run `scripts/submit_bill.mjs`, `scripts/submit_billpayment.mjs`, or batch scripts.
- Global fallback policy for similar unresolved fields/errors:
  - First, ask user to confirm the field.
  - If user does not provide a clear value but insists to continue, upload to Collector with that field left blank.
  - Never fabricate uncertain business values.

## Bill mapping baseline (Wilson 2026-03-02)
- Vendor extraction rule: AI extracts vendor (priority source: `Payment detail 02` -> `Which supplier`), then match against Bill Rule vendor list.
- `Which supplier` parsing supports both layouts:
  - split-cell: key in one cell and value in right-side cell,
  - inline: `Which Supplier: <value>` / `Which Supplier - <value>` / `Which Supplier <value>`.
- If vendor is unmatched in vendor list: ask user to confirm/choose; if user still does not confirm, upload with empty `vendor_ref_text` and let user fix in Collector UI.
- Bill Number: map to `Billing/SOA NO.`.
- Bill Date: map to `Billing End Date`.
- Memo: always include `[AI_AGENT][adv-qbo-tool]` + operator account id (example: Telegram ID).
- Description format (line description):
  - `Feishu: <Billing/SOA NO.>`
  - `Billing Date: <Billing End Date>`
  - `Period Covered: <Month YYYY derived from Billing End Date>`
  - `Business Unit: <Belongs To>`
  - `Reason: <Reasons for payment>`
- Line class: map from `Belongs To` (nullable).
- Location: map from table field `location`; if source `location` is missing/empty, use Collector rule default location value.
- Request No handling:
  - Upload `Request No.` to Collector as submission unique business id (`client_ref`).
  - After QBO bill success, write back QBO `bill_id` to Collector result.
  - Maintain traceability by `request_no <-> bill_id`.

## Use bundled resources
- Config guide: `references/config/quickstart.md`
- API contract summary: `references/config/api-contract.md`
- Runnable scripts:
  - `scripts/parse_payment_request_xlsx.py` (extract business recap fields from uploaded xlsx before confirmation)
  - `scripts/build_match_result.mjs` (build fixed `match_result.json` from parse_result + bill-rules)
  - `scripts/render_confirmation_recap.mjs` (hard-gated confirmation recap from `match_result.json` only)
  - `scripts/refresh_bill_rules.py` (manual bill-rules refresh: snapshot + latest + manifest)
  - `scripts/submit_from_match_result.mjs` (step-4 upload: consume `match_result.json` as single source)
  - `scripts/submit_bill.mjs`
  - `scripts/submit_billpayment.mjs`
  - `scripts/submit_vendor.mjs`
  - `scripts/batch_run.mjs`
  - `scripts/batch_plan_run.mjs`

## Workflow entry policy (hard rule)
- Workflow implementation entry remains: `python skills/adv-qbo-tool/scripts/workflow.py ...`
- For chat/Telegram-safe orchestration, prefer wrappers that delegate to `workflow.py`:
  - `python skills/adv-qbo-tool/scripts/start_chunk_job.py ...`
  - `python skills/adv-qbo-tool/scripts/resume_chunk_job.py`
  - `python skills/adv-qbo-tool/scripts/retry_failed_chunk_job.py`
- Built-in default AI bridge: `scripts/ai_bridge.py` (local OpenClaw Agent session) via `references/config/ai-runtime.json`.
- `node scripts/run_workflow.mjs` is blocked/deprecated.
- Direct step-script invocation is not allowed in normal operation.

## Uploaded Excel handling (mandatory)
- If user uploads `.xlsx`/`.xls`, parse file first and generate business recap from extracted values before asking confirmation.
- Data filter rule: only rows with `status = approved` are valid for extraction and mapping; skip all non-approved rows.
- Use:
```bash
python scripts/parse_payment_request_xlsx.py --file <uploaded.xlsx> --out <parse_result.json>
```
- Then match/suggest fixed option values using Collector Bill Rule dictionaries (Vendor/Category/Location/Tax):
- For fields without explicit deterministic mapping, use AI judgment to propose best match from Bill Rule options (with confidence + rationale + top alternatives).
- Category decision must jointly use signals from:
  1) Payment Detail 01,
  2) Payment Detail 02,
  3) Reasons for payment.
  (Reason-only matching is not sufficient.)
- Category is AI-judgment-driven (not fully rule-programmable):
  - AI must synthesize the three sources above and select from Bill Rule account options.
  - AI should reference international accounting principles/standards when choosing the most appropriate expense category.
  - Output must include recommendation rationale and confidence, with Top3 alternatives.
  - AI is mandatory for Category in workflow (no fallback allowed in production path).
  - Optional history-conditioned context is allowed (vendor/product/reason -> previously confirmed category) as soft preference, not hard rule.
  - Confidence gate policy for category:
    - `confidence >= 0.85`: auto-pass allowed;
    - `0.65 <= confidence < 0.85`: must ask user confirmation;
    - `confidence < 0.65`: confirmation required (do not auto-pass).
```bash
python scripts/suggest_from_bill_rules.py --parsed <parsed.json> --bill-rules <bill-rules.json>
```
- Parse+identify step must produce `parse_result.json` (machine-readable, includes `kind`, `kind_rule_hit`, and `needs_user_kind_confirmation`). Next steps must read this file; do not proceed without it.
- Multi-row handling:
  - parser outputs `records[]` (one record per approved row).
  - if row count > 10, process in chunks of 10 and loop until done.
  - For large Excel files, run bounded workflow batches instead of one long synchronous run:
    - `python skills/adv-qbo-tool/scripts/start_chunk_job.py --file <uploaded.xlsx> --bill-rules <bill_rules.json> --chunk-size 10 --max-batches-per-run 1`
    - continue same job:
      - `python skills/adv-qbo-tool/scripts/resume_chunk_job.py`
    - or resume the most recent waiting chunk job:
      - `python skills/adv-qbo-tool/scripts/resume_chunk_job.py`
  - Do not block the chat waiting for a full large-file run to finish. Start in background, reply immediately with job-started status, then use status/resume helpers.
  - If workflow returns `WAIT_NEXT_BATCH`, summarize the progress for the user and wait for an explicit continue signal before resuming.
  - If the user says `继续`, `继续下一批`, or `go on`, prefer:
    - `python skills/adv-qbo-tool/scripts/chunk_job_status.py`
    - then `python skills/adv-qbo-tool/scripts/resume_chunk_job.py`
  - If the user asks `状态` / `进度`, use:
    - `python skills/adv-qbo-tool/scripts/chunk_job_status.py`
  - After execution, summarize:
    - success count/list
    - failed count/list
    - failure reason summary
    - retry hint for failed items:
      - `python skills/adv-qbo-tool/scripts/retry_failed_chunk_job.py`
  - Progress helper:
    - `python skills/adv-qbo-tool/scripts/chunk_job_status.py`
- Confirmation-stage response hard gate: must be rendered from `match_result.json` only (no legacy recap fallback).
- Step-3 must produce fixed `match_result.json` via script:
```bash
node scripts/build_match_result.mjs --parsed <parse_result.json> --bill-rules <bill-rules.json> --out <match_result.json> --via-workflow true
```
- Optional multi-round user decisions can be merged in step-3:
```bash
node scripts/build_match_result.mjs --parsed <parse_result.json> --bill-rules <bill-rules.json> --user-decisions <user_decisions.json> --out <match_result.json>
```
- Recommended confidence config (record-level trust score):
```bash
node scripts/build_match_result.mjs --parsed <parse_result.json> --bill-rules <bill-rules.json> --confidence-config references/config/confidence-weights.bill.json --out <match_result.json>
```
- `user_decisions.json` example:
```json
{
  "round": 2,
  "asked": ["vendor_ref_text", "category_ref_text"],
  "confirmed": ["vendor_ref_text"],
  "vendor_ref_text": "Wilson Consulting",
  "category_ref_text": "Management Consulting Expense"
}
```
- If parsing succeeds, recap must contain extracted values (not empty template placeholders).
- If some required fields are still missing, ask only those missing business fields.
- Do not ask user for payload path in normal flow.
- **Hard stop rule:** when an xlsx is present, do not proceed to confirmation template or execution until parser output is produced and cited in recap.
- **Hard stop rule:** if parser fails or returns no usable extracted fields, return `parse_failed_or_empty` with a short reason and ask user to re-upload/correct file; do not continue.
- Bill rule dictionaries default strategy: use local/current dictionary first.
- Live pull is an error-handling step (trigger only when match fails/low confidence), then retry matching.
- If live pull fails, continue with latest local cache and label response as `cache_used`.
- Manual refresh trigger (chat intent): when user says `手动更新规则` / `刷新 Bill Rules`, run:
```bash
node skills/adv-qbo-tool/scripts/refresh_bill_rules.mjs --config <company.json> --out-dir <workdir>/rules_cache --set-latest true
```
- Manual refresh output must be reported to user: `changed/hash/snapshot/latest/manifest`.
- Unified rules priority (workflow): `manual_snapshot > rules_cache/latest.json > input --bill-rules`.
- Auto refresh (TTL): when `rules_cache/latest.json` is older than TTL (default 6h), workflow attempts live refresh before matching.
- On live refresh, run taxonomy check (`WHT - Out of scope`, `VAT 12%`, `Non-Taxable`) and record result in manifest/state.
- Tax deterministic mapping (with normalization):
  - `VAT IN` -> `VAT 12%`
  - `VAT EX` -> `Non-Taxable`
  - Normalize case/spacing before match.
  - If VAT flag is unknown/ambiguous, let AI choose best tax option from Bill Rule tax list with confidence+rationale.
- WHT rule (current):
  - Decision is **rate-only**: `has_wht = (wht_rate > 0)`.
  - If `wht_rate <= 0` or missing, set `has_wht=false`.
  - `wht_amount` is retained as reference/context and does not decide existence.
  - Tax affects base derivation for reference checks only.

## Command patterns
Run from skill root.

### Step-4 upload (recommended, file-driven)
```bash
node scripts/submit_from_match_result.mjs --config references/companies/example.json --match-result <match_result.json> --confirmation-received true --via-workflow true
```

### Legacy direct Bill/BillPayment
- Files moved to `scripts/deprecated/submit_bill.mjs` and `scripts/deprecated/submit_billpayment.mjs`.
- Workspace entry scripts `scripts/submit_bill.mjs` / `scripts/submit_billpayment.mjs` are hard-stop stubs and always fail.
- Standard path remains workflow + `submit_from_match_result.mjs` only.

> `references/samples/*` payloads are for local test only, blocked in real runs unless `--allow-sample true` is explicitly passed.

### Vendor (pending to Collector only, no direct QBO submit)
```bash
node scripts/submit_vendor.mjs --config references/companies/example.json --payload references/samples/vendor.pending.sample.json
```

### Batch (multiple records)
```bash
node scripts/batch_run.mjs --config references/companies/example.json --input <user-derived-batch.json> --continue-on-error true --mapping-confirmed true --confirmation-received true
```

### Planned batch (large jobs, chunked)
```bash
node scripts/batch_plan_run.mjs --config references/companies/example.json --input <user-derived-batch.json> --chunk-size 50 --max-items 500 --continue-on-error true --pause-ms 1500 --mapping-confirmed true --confirmation-received true
```

Batch input supports per-item:
- `kind`: `bill` | `bill_payment`
- `payloadPath` (relative to batch file) or inline `payload`
- optional `final` (defaults to false)

Chunked planner behavior:
- Splits large input into chunks (default 50)
- Runs chunk by chunk in serial mode
- Writes checkpoint file for progress/resume
- Default policy: continue-on-error and report all failures at the end

## Output contract
### Before user confirmation (planning stage)
Return business-facing recap only (no command lines / no file paths / no sample file hints by default):
- `confirmation_required` = true
- `kind`
- `recap` with concrete bill values user can verify
- **Mandatory interaction format (hard rule):** always output full-row/full-record recap, not unresolved-only summary.
  - For every record, include all core fields (Vendor, Bill Number, Request No, Bill/Due Date, Location, Category, Tax, WHT, Class, confidence).
  - Mark each field with status: `resolved` / `needs_confirmation` / `default_fallback`.
  - Unresolved fields must include suggestions (Top3 + confidence + rationale) when dictionaries are available.
  - Do not hide resolved fields even when only one field needs confirmation.
- `next_action` = `wait_for_user_confirmation`

If source file is missing, ask the user for business fields directly (vendor/date/lines/category/class/location/WHT), not for payload path.
Only include technical details (script args, payload path, command) when user explicitly asks for debug details.

### After user confirmation (execution stage)
Return:
- `submission_id`
- `status`
- `validate_ok`
- `precheck_ok`
- `manual_submit_required` (always true)
- `ai_agent_tag`
- `view_url`
