import { parseArgs, readJson, createSession } from './_client.mjs';

function normalizePayload(input) {
  if (input?.payload && typeof input.payload === 'object') return input.payload;
  const out = { ...input };
  delete out.kind;
  delete out.client_ref;
  delete out.memo;
  delete out.company_key;
  delete out.tenant_id;
  return out;
}

function tagMemo(memo) {
  const raw = String(memo || '').trim();
  if (raw.includes('[AI_AGENT][adv-qbo-tool]')) return raw;
  return `[AI_AGENT][adv-qbo-tool] ${raw}`.trim();
}

async function createDraftWithRetry(api, kind, clientRef, memo) {
  try {
    return await api.postJson('/api/submissions', { kind, client_ref: clientRef, memo });
  } catch (e) {
    const msg = String(e?.message || e);
    if (!msg.includes('client_ref_not_unique')) throw e;
    const retryRef = `${clientRef}-${Date.now().toString().slice(-6)}`;
    const out = await api.postJson('/api/submissions', { kind, client_ref: retryRef, memo });
    out._used_client_ref = retryRef;
    return out;
  }
}

const args = parseArgs(process.argv);
if (String(args['allow-legacy-direct'] || '').toLowerCase() !== 'true') {
  throw new Error('legacy_direct_disabled: use scripts/submit_from_match_result.mjs (pass --allow-legacy-direct true only for emergency/manual debug)');
}
if (String(args['confirmation-received'] || '').toLowerCase() !== 'true') {
  throw new Error('confirmation_required: pass --confirmation-received true only after explicit user approval');
}
if (!args.payload) {
  throw new Error('payload_required: pass --payload <json-file-from-user-data>');
}
if (String(args.payload).includes('references/samples') && String(args['allow-sample'] || '').toLowerCase() !== 'true') {
  throw new Error('sample_payload_blocked: do not use references/samples in real runs');
}
const cfg = readJson(args.config);
const input = readJson(args.payload);
const base = cfg.base_url || 'https://qb.uudc.us';

const initialClientRef = String(input.client_ref || input.bill_no || `bill-${Date.now()}`);
const memo = tagMemo(input.memo || '');
const payload = normalizePayload(input);

const api = await createSession(base, cfg);

const create = await createDraftWithRetry(api, 'bill', initialClientRef, memo);
const id = create?.row?.id;
if (!id) throw new Error('create_submission_missing_id');
const finalClientRef = create?._used_client_ref || initialClientRef;

await api.putJson(`/api/submissions/${encodeURIComponent(id)}`, {
  client_ref: finalClientRef,
  memo,
  payload
});

const validate = await api.postJson(`/api/submissions/${encodeURIComponent(id)}/validate`, {});
const precheck = await api.postJson(`/api/submissions/${encodeURIComponent(id)}/precheck`, {});
const latest = await api.getJson(`/api/submissions/${encodeURIComponent(id)}`);

console.log(JSON.stringify({
  submission_id: id,
  kind: 'bill',
  client_ref: finalClientRef,
  status: latest?.row?.status,
  qbo_id: latest?.row?.result?.qbo_id || null,
  validate_ok: !!validate?.ok,
  precheck_ok: !!precheck?.ok,
  submit_ok: false,
  manual_submit_required: true,
  ai_agent_tag: '[AI_AGENT][adv-qbo-tool]',
  view_url: `${base}/submissions/${id}/edit`
}, null, 2));
