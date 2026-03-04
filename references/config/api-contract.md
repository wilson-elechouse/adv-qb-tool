# API Contract (V3 Web API)

Workflow used by scripts:

1) Login session
- `POST /api/auth/login` body: `{ username, password }`

2) Select tenant/company
- `POST /api/tenant/select` body: `{ tenantId }`

3) Create draft
- `POST /api/submissions` body: `{ kind, client_ref, memo }`

4) Update payload
- `PUT /api/submissions/:id` body: `{ client_ref, memo, payload }`

5) Validate / precheck / submit
- `POST /api/submissions/:id/validate`
- `POST /api/submissions/:id/precheck`
- `POST /api/submissions/:id/submit` (final QBO posting)

6) Read latest status/result
- `GET /api/submissions/:id`

7) Vendor pending queue (skill writes to Collector only)
- `GET /api/bill-rules`
- `POST /api/bill-rules` (update `qboOptionDictionaries.vendors` with pending markers)

Kinds:
- `bill`
- `bill_payment`
- `vendor_pending` (skill-level output kind; no QBO submit)

Behavior contract (agent-side):
- If required data is missing/invalid/ambiguous, stop and ask user for confirmation (no silent defaulting).
- If source file mapping is incomplete/uncertain, produce mapping questions and wait for user confirmation before calling create endpoints.
- Do not auto-correct business fields (example: date conflicts) without explicit user approval.
- For XNOFI tenant, prefer rule file `references/config/category-mapping.xnofi.json`.
- Enforce guard list from `references/config/confirmation-guards.xnofi.json` before create calls.
- Always keep auditable artifacts for each batch run: mapping preview, precheck report, and confirmation questions.
