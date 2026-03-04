import { parseArgs, readJson, createSession } from './_client.mjs';

function tagMemo(memo, confidence) {
  const raw = String(memo || '').trim();
  const withTag = raw.includes('[AI_AGENT][adv-qbo-tool]') ? raw : `[AI_AGENT][adv-qbo-tool] ${raw}`.trim();
  if (!confidence) return withTag;
  const c = `confidence=${confidence.level || 'unknown'}:${confidence.score ?? ''}`;
  return `${withTag} [${c}]`.trim();
}

function parseApiError(e) {
  const msg = String(e?.message || e || '');
  const m = msg.match(/\{[\s\S]*\}$/);
  let body = null;
  if (m) {
    try { body = JSON.parse(m[0]); } catch {}
  }
  return { msg, body };
}

function isClientRefConflict(errObj) {
  const t = `${errObj?.msg || ''} ${JSON.stringify(errObj?.body || {})}`;
  return /client_ref_not_unique|client ref.*unique|duplicate.*client[_\s-]?ref/i.test(t);
}

function pickExistingId(errObj) {
  const b = errObj?.body || {};
  return b?.existing_submission_id || b?.submission_id || b?.row?.id || null;
}

function pickExistingViewPath(errObj) {
  const b = errObj?.body || {};
  return b?.existing_view_path || null;
}

function normalizeText(s) {
  return String(s || '').trim().toLowerCase();
}

function hasPlaceholderId(v) {
  const s = String(v || '').trim();
  return !s || s === '__SET_BY_TENANT__';
}

function assertBillRulesReadyForPayload(billRules, payload) {
  const d = billRules?.rules?.qboOptionDictionaries || {};
  const taxDict = Array.isArray(d.taxCodes) ? d.taxCodes : [];
  const accountDict = Array.isArray(d.accounts) ? d.accounts : [];

  const taxByLabel = new Map(taxDict.map(x => [normalizeText(x?.label), x]));
  const acctByLabel = new Map(accountDict.map(x => [normalizeText(x?.label), x]));
  const acctByKey = new Map(accountDict.map(x => [normalizeText(x?.key), x]));

  const lines = Array.isArray(payload?.lines) ? payload.lines : [];
  const issues = [];

  for (let i = 0; i < lines.length; i += 1) {
    const ln = lines[i] || {};
    const taxVal = ln.tax_ref_text ?? ln.tax_code_ref_text ?? '';
    if (String(taxVal).trim()) {
      const hit = taxByLabel.get(normalizeText(taxVal));
      if (!hit) issues.push(`line${i + 1}.tax_not_in_bill_rules_label_only:${taxVal}`);
      else if (hasPlaceholderId(hit.qbo_tax_code_id)) issues.push(`line${i + 1}.tax_not_materialized:${hit.label || hit.key}`);
    }

    const acctVal = ln.account_ref_text ?? '';
    if (String(acctVal).trim()) {
      const ahit = acctByLabel.get(normalizeText(acctVal)) || acctByKey.get(normalizeText(acctVal));
      if (!ahit) issues.push(`line${i + 1}.account_not_in_bill_rules:${acctVal}`);
    }
  }

  if (issues.length) {
    const e = new Error(`bill_rules_not_materialized_or_unmapped: ${issues.join(', ')}`);
    e.code = 'bill_rules_not_materialized_or_unmapped';
    throw e;
  }
}

const args = parseArgs(process.argv);
if (String(args['via-workflow'] || '').toLowerCase() !== 'true') {
  throw new Error('workflow_entry_required: submit_from_match_result must be called via run_workflow.mjs');
}
if (String(args['confirmation-received'] || '').toLowerCase() !== 'true') {
  throw new Error('confirmation_required: pass --confirmation-received true only after explicit user approval');
}
if (!args['match-result']) {
  throw new Error('match_result_required: pass --match-result <match_result.json>');
}

const cfg = readJson(args.config);
const match = readJson(args['match-result']);
const base = cfg.base_url || 'https://qb.uudc.us';

if (!match?.ok) throw new Error('invalid_match_result: ok=false');
if (!['ready', 'ready_with_blanks'].includes(String(match?.status || ''))) {
  throw new Error('match_result_not_ready: status must be ready|ready_with_blanks');
}
if (!match?.collector_payload?.draft) {
  throw new Error('match_result_missing_payload: collector_payload.draft is required');
}

const kind = String(match.kind || match.collector_payload.kind || '').toLowerCase();
if (!['bill', 'bill_payment'].includes(kind)) {
  throw new Error('invalid_kind_in_match_result: expected bill|bill_payment');
}

const payload = match.collector_payload.draft;
const clientRef = String(payload.client_ref || '').trim();
if (!clientRef) {
  throw new Error('client_ref_required: client_ref must equal request number and cannot be empty');
}
const memo = tagMemo(payload.memo || '', payload.ai_confidence || match.confidence);

const onConflict = String(args['on-client-ref-conflict'] || 'block').toLowerCase();
const api = await createSession(base, cfg);

if (kind === 'bill') {
  const rules = await api.getJson('/api/bill-rules');
  assertBillRulesReadyForPayload(rules, payload);
}

let id = null;
try {
  const create = await api.postJson('/api/submissions', { kind, client_ref: clientRef, memo });
  id = create?.row?.id;
  if (!id) throw new Error('create_submission_missing_id');
} catch (e) {
  const pe = parseApiError(e);
  if (!isClientRefConflict(pe)) throw e;

  const existingId = String(pickExistingId(pe) || '').trim() || null;
  const existingViewPath = pickExistingViewPath(pe);
  const existingViewUrl = existingViewPath
    ? `${base}${existingViewPath}`
    : (existingId ? `${base}/submissions/${existingId}/edit` : null);

  console.log(JSON.stringify({
    ok: false,
    needs_user_action: true,
    needs_user_decision: true,
    action_required: 'delete_existing_submission',
    conflict: 'client_ref_not_unique',
    client_ref: clientRef,
    kind,
    existing_submission_id: existingId,
    existing_view_url: existingViewUrl,
    requested_conflict_mode: onConflict,
    message: 'An existing submission with the same client_ref already exists. Delete the existing submission first, then retry.'
  }, null, 2));
  process.exit(0);
}

await api.putJson(`/api/submissions/${encodeURIComponent(id)}`, {
  client_ref: clientRef,
  memo,
  payload
});

const validate = await api.postJson(`/api/submissions/${encodeURIComponent(id)}/validate`, {});
const precheck = await api.postJson(`/api/submissions/${encodeURIComponent(id)}/precheck`, {});
const latest = await api.getJson(`/api/submissions/${encodeURIComponent(id)}`);

console.log(JSON.stringify({
  ok: true,
  submission_id: id,
  kind,
  client_ref: clientRef,
  status: latest?.row?.status,
  qbo_id: latest?.row?.result?.qbo_id || null,
  validate_ok: !!validate?.ok,
  precheck_ok: !!precheck?.ok,
  submit_ok: false,
  manual_submit_required: true,
  ai_agent_tag: '[AI_AGENT][adv-qbo-tool]',
  confidence: match.confidence || payload.ai_confidence || null,
  view_url: `${base}/submissions/${id}/edit`
}, null, 2));
