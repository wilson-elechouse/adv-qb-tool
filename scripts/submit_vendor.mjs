import { parseArgs, readJson, createSession } from './_client.mjs';

function tagMemo(memo) {
  const raw = String(memo || '').trim();
  if (raw.includes('[AI_AGENT][adv-qbo-tool]')) return raw;
  return `[AI_AGENT][adv-qbo-tool] ${raw}`.trim();
}

function normName(s) {
  return String(s || '').trim().replace(/\s+/g, ' ');
}

function toArray(input) {
  if (Array.isArray(input?.vendors)) return input.vendors;
  if (Array.isArray(input)) return input;
  if (input?.name || input?.display_name || input?.vendor_name) return [input];
  return [];
}

function toPendingVendorItem(v, idx) {
  const name = normName(v?.name || v?.display_name || v?.vendor_name || '');
  if (!name) return null;
  const note = String(v?.note || '').trim();
  return {
    key: `PENDING_VENDOR_${Date.now()}_${idx + 1}`,
    label: name,
    qbo_vendor_id: '__PENDING_MANUAL_CREATE__',
    active: true,
    pending_qbo_create: true,
    source: 'ai_agent_skill',
    note,
    ai_agent_tag: '[AI_AGENT][adv-qbo-tool]'
  };
}

const args = parseArgs(process.argv);
const cfg = readJson(args.config);
const input = readJson(args.payload);
const base = cfg.base_url || 'https://qb.uudc.us';

const memo = tagMemo(input?.memo || 'vendor submit to collector as pending');
const candidates = toArray(input)
  .map((x, i) => toPendingVendorItem(x, i))
  .filter(Boolean);

if (!candidates.length) throw new Error('vendor_candidates_required');

const api = await createSession(base, cfg);

const current = await api.getJson('/api/bill-rules');
if (!current?.ok || !current?.rules) throw new Error('load_bill_rules_failed');

const rules = current.rules || {};
rules.qboOptionDictionaries = rules.qboOptionDictionaries || {};
const oldVendors = Array.isArray(rules.qboOptionDictionaries.vendors) ? rules.qboOptionDictionaries.vendors : [];

const byName = new Map();
for (const v of oldVendors) {
  const key = normName(v?.label || v?.name || '').toLowerCase();
  if (key) byName.set(key, v);
}

const appended = [];
for (const v of candidates) {
  const key = normName(v.label).toLowerCase();
  if (!key || byName.has(key)) continue;
  byName.set(key, v);
  appended.push(v);
}

rules.qboOptionDictionaries.vendors = Array.from(byName.values());
rules.meta = rules.meta || {};
rules.meta.vendor_pending_queue = {
  updated_at: new Date().toISOString(),
  memo,
  appended_count: appended.length,
  last_source: 'ai_agent_skill'
};

await api.postJson('/api/bill-rules', { rules });

console.log(JSON.stringify({
  ok: true,
  kind: 'vendor_pending',
  tenant_id: cfg.tenant_id || null,
  pushed_count: appended.length,
  skipped_existing_count: candidates.length - appended.length,
  manual_submit_required: true,
  ai_agent_tag: '[AI_AGENT][adv-qbo-tool]',
  next_step: 'Open Collector Settings > Vendor Management and manually create/update vendor in QBO.',
  review_url: `${base}/settings/vendors`
}, null, 2));
