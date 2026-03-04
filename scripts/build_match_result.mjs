import fs from 'node:fs';
import path from 'node:path';
import { parseArgs, readJson } from './_client.mjs';

function norm(s) {
  return String(s || '').trim().toLowerCase().replace(/\s+/g, ' ');
}

function toNum(v) {
  const s = String(v ?? '').replace(/,/g, '').trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function toPct(v) {
  const s = String(v ?? '').replace('%', '').trim();
  if (!s) return null;
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return n > 1 ? n / 100 : n;
}

function matchOne(source, options = []) {
  const src = norm(source);
  if (!src || !options.length) return { value: '', score: 0, top3: [] };
  const scored = options
    .map((v) => String(v || '').trim())
    .filter(Boolean)
    .map((v) => {
      const nv = norm(v);
      let score = 0;
      if (nv === src) score = 1;
      else if (nv.includes(src) || src.includes(nv)) score = 0.75;
      else {
        const srcSet = new Set(src.split(' '));
        const vSet = new Set(nv.split(' '));
        let common = 0;
        for (const x of srcSet) if (vSet.has(x)) common += 1;
        score = common / Math.max(srcSet.size, vSet.size, 1);
      }
      return { value: v, score: Number(score.toFixed(3)) };
    })
    .sort((a, b) => b.score - a.score);
  return { value: scored[0]?.value || '', score: scored[0]?.score || 0, top3: scored.slice(0, 3).map((x) => x.value) };
}

function dictValues(arr = []) {
  return arr.map((x) => (x?.label || x?.key || '')).filter(Boolean);
}

function dictRefIds(arr = [], entity, idField) {
  const out = {};
  for (const item of arr) {
    const label = String(item?.label || item?.key || '').trim();
    const qboId = String(item?.[idField] || '').trim();
    if (!label || !qboId || qboId.startsWith('__')) continue;
    out[norm(label)] = `${entity}:${qboId}`;
  }
  return out;
}

function refIdFor(refMap = {}, value) {
  return refMap[norm(value)] || '';
}

function parseRules(raw) {
  const rules = raw?.rules || raw || {};
  const d = rules?.qboOptionDictionaries || {};
  return {
    vendors: dictValues(d.vendors),
    vendorRefIds: dictRefIds(d.vendors, 'vendor', 'qbo_vendor_id'),
    accounts: dictValues(d.accounts),
    accountRefIds: dictRefIds(d.accounts, 'account', 'qbo_account_id'),
    locations: dictValues(d.locations),
    locationRefIds: dictRefIds(d.locations, 'department', 'qbo_department_id'),
    taxes: dictValues(d.taxCodes),
    taxRefIds: dictRefIds(d.taxCodes, 'taxcode', 'qbo_tax_code_id'),
    classes: dictValues(d.classes),
    classRefIds: dictRefIds(d.classes, 'class', 'qbo_class_id'),
  };
}

function fmtDateLong(s) {
  const d = new Date(String(s || ''));
  if (Number.isNaN(d.getTime())) return String(s || '');
  return d.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
}

function fmtMonthYear(s) {
  const d = new Date(String(s || ''));
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
}

function extractGrossFromPd01(pd01) {
  const t = String(pd01 || '');
  const m = t.match(/payables\s*amount\s*-?\s*gross\s*:\s*([0-9][0-9,]*(?:\.\d+)?)/i);
  if (!m) return null;
  return toNum(m[1]);
}

function pickWhtOutOfScopeTax(localTaxes = [], liveTaxes = []) {
  const candidates = [...(localTaxes || []), ...(liveTaxes || [])].filter(Boolean);
  if (!candidates.length) return '';
  const direct = candidates.find((x) => /wht\s*-\s*out\s*of\s*scope/i.test(String(x)));
  if (direct) return direct;
  const byToken = candidates.find((x) => /wht/i.test(String(x)) && /out\s*of\s*scope/i.test(String(x)));
  if (byToken) return byToken;
  const lexical = matchOne('WHT-Out of scope', candidates);
  return lexical.score >= 0.55 ? lexical.value : '';
}

function ensureDir(p) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
}

const args = parseArgs(process.argv);
if (String(args['via-workflow'] || '').toLowerCase() !== 'true') {
  throw new Error('workflow_entry_required: build_match_result must be called via run_workflow.mjs');
}
if (!args.parsed || !args['bill-rules'] || !args.out) {
  console.error('usage: node scripts/build_match_result.mjs --parsed <parse_result.json> --bill-rules <bill-rules.json> --out <match_result.json> [--user-decisions <json>] [--live-rules <json>] [--force-kind bill|bill_payment] [--continue-on-unresolved true] [--confidence-config <json>] --via-workflow true');
  process.exit(1);
}

const parsed = readJson(args.parsed);
const localRules = parseRules(readJson(args['bill-rules']));
const liveRules = args['live-rules'] ? parseRules(readJson(args['live-rules'])) : null;
const confidenceCfg = args['confidence-config'] ? readJson(args['confidence-config']) : {
  eps: 1e-6,
  missingPenaltyBase: 0.9,
  levels: { highMin: 0.85, mediumMin: 0.65 },
  weights: {
    vendor_ref_text: 0.20,
    category_ref_text: 0.20,
    tax_code_ref_text: 0.12,
    amount_lines: 0.15,
    bill_no: 0.08,
    client_ref: 0.08,
    bill_date: 0.07,
    due_date: 0.05,
    location_ref_text: 0.05,
  }
};
const userDecisions = args['user-decisions'] ? readJson(args['user-decisions']) : {};
const continueOnUnresolved = String(args['continue-on-unresolved'] || 'false') === 'true';

const recap = parsed.recap || {};
const kind = args['force-kind'] || parsed.kind || 'unknown';
const needsKindConfirm = kind === 'unknown' || parsed.needs_user_kind_confirmation === true;

const effectiveRules = { ...localRules };

const vendorSource = recap.vendor || recap.reason || '';
let vendorMatch = matchOne(vendorSource, effectiveRules.vendors);
let vendorLiveRetried = false;
if (vendorMatch.score < 0.55 && liveRules?.vendors?.length) {
  vendorMatch = matchOne(vendorSource, liveRules.vendors);
  vendorLiveRetried = true;
}

const categorySource = [
  recap.reason || '',
  recap.payment_detail_01_text || '',
  recap.payment_detail_02_text || ''
].filter(Boolean).join(' | ');
let categoryMatch = matchOne(categorySource, effectiveRules.accounts);
let categoryLiveRetried = false;
if (categoryMatch.score < 0.55 && liveRules?.accounts?.length) {
  categoryMatch = matchOne(categorySource, liveRules.accounts);
  categoryLiveRetried = true;
}

const locationSource = recap.location || recap.project_type || '';
let locationMatch = matchOne(locationSource, effectiveRules.locations);
let locationLiveRetried = false;
if (locationSource && locationMatch.score < 0.55 && liveRules?.locations?.length) {
  locationMatch = matchOne(locationSource, liveRules.locations);
  locationLiveRetried = true;
}

const classSource = recap.belongs_to || '';
let classMatch = matchOne(classSource, effectiveRules.classes || []);
if (classSource && classMatch.score < 0.55 && liveRules?.classes?.length) {
  classMatch = matchOne(classSource, liveRules.classes);
  classMatch.liveRetried = true;
}

const vatFlag = norm(recap.vat_flag);
let taxDesired = '';
if (['vat in', 'vatin', 'in', 'vat in/ex: in'].includes(vatFlag)) taxDesired = '12% S - Goods';
else if (['vat ex', 'vatex', 'ex', 'vat in/ex: ex'].includes(vatFlag)) taxDesired = '0% Z';

let taxMatch;
if (taxDesired) {
  taxMatch = matchOne(taxDesired, effectiveRules.taxes);
  if (taxMatch.score < 1 && liveRules?.taxes?.length) {
    taxMatch = matchOne(taxDesired, liveRules.taxes);
    taxMatch.liveRetried = true;
  }
} else {
  taxMatch = matchOne(recap.reason || '', effectiveRules.taxes);
  if (taxMatch.score < 0.55 && liveRules?.taxes?.length) {
    taxMatch = matchOne(recap.reason || '', liveRules.taxes);
    taxMatch.liveRetried = true;
  }
}

const whtRate = toPct(recap.wht_rate);
const whtAmount = toNum(recap.wht_amount);
const grossBase = (recap.lines || []).map((x) => toNum(x?.amount)).filter((x) => x != null).reduce((a, b) => a + b, 0);
const taxPct = /12\s*%/.test(String(taxMatch.value || '')) ? 0.12 : (/non|exempt|out of scope/i.test(String(taxMatch.value || '')) ? 0 : null);
const netBase = taxPct == null ? grossBase : grossBase / (1 + taxPct);
const expectedWht = whtRate == null ? null : +(netBase * whtRate).toFixed(2);
const consistent = (expectedWht != null && whtAmount != null) ? Math.abs(expectedWht - whtAmount) <= 0.1 : null;

const fields = {
  vendor_ref_text: {
    value: userDecisions.vendor_ref_text ?? (vendorMatch.score >= 0.55 ? vendorMatch.value : ''),
    ref_id: refIdFor(localRules.vendorRefIds, userDecisions.vendor_ref_text ?? (vendorMatch.score >= 0.55 ? vendorMatch.value : '')) || refIdFor(liveRules?.vendorRefIds, userDecisions.vendor_ref_text ?? (vendorMatch.score >= 0.55 ? vendorMatch.value : '')),
    source: userDecisions.vendor_ref_text ? 'user' : 'ai+dictionary',
    confidence: userDecisions.vendor_ref_text ? 1 : vendorMatch.score,
    matched: vendorMatch.score >= 0.55,
    alternatives: vendorMatch.top3 || [],
    needs_user_confirmation: !userDecisions.vendor_ref_text && vendorMatch.score < 0.55,
    live_retry_used: vendorLiveRetried,
    allow_blank_on_user_insist: true,
  },
  client_ref: {
    value: recap.request_no || '',
    source: 'rule:Request No.',
    confidence: 1,
    needs_user_confirmation: false,
  },
  bill_no: {
    value: recap.bill_number || '',
    source: 'rule:Billing/SOA NO.',
    confidence: 1,
    needs_user_confirmation: false,
  },
  bill_date: {
    value: recap.bill_date || recap.billing_end_date || '',
    source: 'rule:Billing End Date',
    confidence: 1,
    needs_user_confirmation: false,
  },
  due_date: {
    value: recap.due_date || '',
    source: 'rule:Due Date',
    confidence: recap.due_date ? 1 : 0,
    needs_user_confirmation: !recap.due_date,
  },
  category_ref_text: {
    value: userDecisions.category_ref_text ?? (categoryMatch.score >= 0.55 ? categoryMatch.value : ''),
    ref_id: refIdFor(localRules.accountRefIds, userDecisions.category_ref_text ?? (categoryMatch.score >= 0.55 ? categoryMatch.value : '')) || refIdFor(liveRules?.accountRefIds, userDecisions.category_ref_text ?? (categoryMatch.score >= 0.55 ? categoryMatch.value : '')),
    source: userDecisions.category_ref_text ? 'user' : 'ai+dictionary',
    confidence: userDecisions.category_ref_text ? 1 : categoryMatch.score,
    alternatives: categoryMatch.top3 || [],
    needs_user_confirmation: !userDecisions.category_ref_text && categoryMatch.score < 0.55,
    live_retry_used: categoryLiveRetried,
    allow_blank_on_user_insist: true,
  },
  location_ref_text: {
    value: userDecisions.location_ref_text ?? (locationMatch.score >= 0.55 ? locationMatch.value : ''),
    ref_id: refIdFor(localRules.locationRefIds, userDecisions.location_ref_text ?? (locationMatch.score >= 0.55 ? locationMatch.value : '')) || refIdFor(liveRules?.locationRefIds, userDecisions.location_ref_text ?? (locationMatch.score >= 0.55 ? locationMatch.value : '')),
    source: userDecisions.location_ref_text ? 'user' : 'rule_or_ai+dictionary',
    confidence: userDecisions.location_ref_text ? 1 : locationMatch.score,
    alternatives: locationMatch.top3 || [],
    needs_user_confirmation: !!locationSource && !userDecisions.location_ref_text && locationMatch.score < 0.55,
    fallback: 'collector_default_if_empty',
    live_retry_used: locationLiveRetried,
    allow_blank_on_user_insist: true,
    use_collector_default_when_empty: !(userDecisions.location_ref_text ?? (locationMatch.score >= 0.55 ? locationMatch.value : '')),
  },
  tax_code_ref_text: {
    value: userDecisions.tax_code_ref_text ?? (taxMatch.score >= 0.55 ? taxMatch.value : ''),
    ref_id: refIdFor(localRules.taxRefIds, userDecisions.tax_code_ref_text ?? (taxMatch.score >= 0.55 ? taxMatch.value : '')) || refIdFor(liveRules?.taxRefIds, userDecisions.tax_code_ref_text ?? (taxMatch.score >= 0.55 ? taxMatch.value : '')),
    source: userDecisions.tax_code_ref_text ? 'user' : 'rule_tax_list_match',
    confidence: userDecisions.tax_code_ref_text ? 1 : taxMatch.score || 0,
    alternatives: taxMatch.top3 || [],
    needs_user_confirmation: !userDecisions.tax_code_ref_text && (taxDesired ? (taxMatch.score < 1) : ((taxMatch.score || 0) < 0.55)),
    allow_blank_on_user_insist: true,
    match_evidence: { desired: taxDesired || null, matched_value: taxMatch.value || '', score: taxMatch.score || 0 }
  },
  class_ref_text: {
    value: userDecisions.class_ref_text ?? (classMatch.score >= 0.55 ? classMatch.value : ''),
    ref_id: refIdFor(localRules.classRefIds, userDecisions.class_ref_text ?? (classMatch.score >= 0.55 ? classMatch.value : '')) || refIdFor(liveRules?.classRefIds, userDecisions.class_ref_text ?? (classMatch.score >= 0.55 ? classMatch.value : '')),
    source: userDecisions.class_ref_text ? 'user' : 'rule_class_list_match',
    confidence: userDecisions.class_ref_text ? 1 : classMatch.score || 0,
    alternatives: classMatch.top3 || [],
    needs_user_confirmation: !userDecisions.class_ref_text && (!!classSource && (classMatch.score < 1)),
    allow_blank_on_user_insist: true,
    match_evidence: { source_value: classSource || null, matched_value: classMatch.value || '', score: classMatch.score || 0 }
  },
  withholding_tax: {
    has_wht: whtRate != null || whtAmount != null,
    rate: whtRate,
    amount: whtAmount,
    base: { gross: +grossBase.toFixed(2), net: +netBase.toFixed(2), tax_pct_used: taxPct },
    expected_amount_from_base: expectedWht,
    consistency_check: { tolerance: 0.1, pass: consistent },
    source: 'rule+program',
    needs_user_confirmation: (whtRate != null && whtAmount == null) || (whtRate == null && whtAmount != null) || consistent === false,
    allow_blank_on_user_insist: true,
  },
};

const unresolved = Object.entries(fields)
  .filter(([, v]) => v && typeof v === 'object' && v.needs_user_confirmation)
  .map(([k]) => k);

function clamp01(x) {
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(1, x));
}

function fieldScore(v) {
  if (!v || typeof v !== 'object') return 0.4;
  if (v.blank_by_policy) return 0.2;
  if (v.source === 'user' || String(v.source || '').startsWith('rule:')) return 1.0;
  const c = Number(v.confidence ?? 0);
  if (c >= 0.85) return 0.85;
  if (c >= 0.55) return 0.65;
  return 0.4;
}

const scoreWeights = confidenceCfg.weights || {};

const amountLinesScore = (recap.lines || []).length > 0 ? 1.0 : 0.4;
const weightedParts = [
  ['vendor_ref_text', fieldScore(fields.vendor_ref_text)],
  ['category_ref_text', fieldScore(fields.category_ref_text)],
  ['tax_code_ref_text', fieldScore(fields.tax_code_ref_text)],
  ['amount_lines', amountLinesScore],
  ['bill_no', fieldScore(fields.bill_no)],
  ['client_ref', fieldScore(fields.client_ref)],
  ['bill_date', fieldScore(fields.bill_date)],
  ['due_date', fieldScore(fields.due_date)],
  ['location_ref_text', fieldScore(fields.location_ref_text)],
];

const EPS = Number(confidenceCfg.eps || 1e-6);
let lnSum = 0;
for (const [k, sRaw] of weightedParts) {
  const w = scoreWeights[k] || 0;
  const s = clamp01(sRaw);
  lnSum += w * Math.log(Math.max(s, EPS));
}
const scoreBase = Math.exp(lnSum);

const keyMissing = [
  !fields.vendor_ref_text?.value,
  !fields.category_ref_text?.value,
  !fields.tax_code_ref_text?.value,
  !fields.bill_date?.value,
].filter(Boolean).length;
const penaltyBase = Number(confidenceCfg.missingPenaltyBase || 0.9);
const penalty = Math.pow(penaltyBase, keyMissing);
const billConfidenceScore = clamp01(scoreBase * penalty);
const highMin = Number(confidenceCfg.levels?.highMin ?? 0.85);
const mediumMin = Number(confidenceCfg.levels?.mediumMin ?? 0.65);
const billConfidenceLevel = billConfidenceScore >= highMin ? 'high' : (billConfidenceScore >= mediumMin ? 'medium' : 'low');

if (continueOnUnresolved) {
  for (const k of unresolved) {
    if (fields[k] && typeof fields[k] === 'object' && 'value' in fields[k] && !fields[k].value) {
      fields[k].blank_by_policy = true;
    }
  }
}

const status = needsKindConfirm
  ? 'needs_user_confirmation'
  : (unresolved.length ? (continueOnUnresolved ? 'ready_with_blanks' : 'needs_user_confirmation') : 'ready');

const localDictCount = (localRules.vendors?.length || 0) + (localRules.accounts?.length || 0) + (localRules.locations?.length || 0) + (localRules.taxes?.length || 0);
const liveDictCount = (liveRules?.vendors?.length || 0) + (liveRules?.accounts?.length || 0) + (liveRules?.locations?.length || 0) + (liveRules?.taxes?.length || 0);
const bill_rules_source = liveDictCount > 0 && (vendorLiveRetried || categoryLiveRetried || locationLiveRetried || taxMatch.liveRetried)
  ? 'live_retry'
  : (localDictCount > 0 ? 'local' : (liveDictCount > 0 ? 'live_only' : 'unavailable'));

const lineDescription = [
  `Feishu: ${recap.bill_number || ''}`,
  `Billing Date: ${fmtDateLong(recap.billing_end_date || recap.bill_date || '')}`,
  `Period Covered: ${fmtMonthYear(recap.billing_end_date || recap.bill_date || '')}`,
  `Business Unit: ${recap.belongs_to || ''}`,
  `Reason: ${recap.reason || ''}`,
].join('; ');

const fallbackAmt = extractGrossFromPd01(recap.payment_detail_01_text);
const sourceLines = Array.isArray(recap.lines) ? recap.lines : [];
const businessLines = sourceLines.length
  ? sourceLines
      .map((ln) => ({
        amount: toNum(ln?.amount),
        account_ref_text: fields.category_ref_text.value,
        account_ref_id: fields.category_ref_text.ref_id || '',
        tax_ref_text: fields.tax_code_ref_text.value,
        tax_ref_id: fields.tax_code_ref_text.ref_id || '',
        class_ref_text: fields.class_ref_text.value,
        class_ref_id: fields.class_ref_text.ref_id || '',
        description: lineDescription,
        meta: { kind: 'business' },
      }))
      .filter((ln) => ln.amount != null && ln.amount > 0)
  : ((fallbackAmt != null && fallbackAmt > 0)
      ? [{
          amount: fallbackAmt,
          account_ref_text: fields.category_ref_text.value,
          account_ref_id: fields.category_ref_text.ref_id || '',
          tax_ref_text: fields.tax_code_ref_text.value,
          tax_ref_id: fields.tax_code_ref_text.ref_id || '',
          class_ref_text: fields.class_ref_text.value,
          class_ref_id: fields.class_ref_text.ref_id || '',
          description: lineDescription,
          meta: { kind: 'business' },
        }]
      : []);

const businessSum = businessLines.reduce((a, b) => a + Number(b.amount || 0), 0);
const netBaseForWht = Number(fields.withholding_tax?.base?.net ?? businessSum);
const whtRateNorm = toPct(fields.withholding_tax?.rate);
const whtAmountAbs = toNum(fields.withholding_tax?.amount);
const computedWht = whtAmountAbs != null ? Math.abs(whtAmountAbs) : (whtRateNorm != null ? +(netBaseForWht * whtRateNorm).toFixed(2) : null);
const resolvedWhtAmount = computedWht != null ? +Math.abs(computedWht).toFixed(2) : null;
const whtTaxRef = pickWhtOutOfScopeTax(localRules.taxes, liveRules?.taxes || []) || fields.tax_code_ref_text.value || '';
const whtLine = (fields.withholding_tax?.has_wht && computedWht != null && computedWht > 0)
  ? {
      amount: -Math.abs(computedWht),
      description: 'Withholding tax',
      tax_ref_text: whtTaxRef,
      tax_ref_id: refIdFor(localRules.taxRefIds, whtTaxRef) || refIdFor(liveRules?.taxRefIds, whtTaxRef) || '',
      class_ref_text: '',
      class_ref_id: '',
      account_ref_text: 'EWT Payable-BIR',
      account_ref_id: refIdFor(localRules.accountRefIds, 'EWT Payable-BIR') || refIdFor(liveRules?.accountRefIds, 'EWT Payable-BIR') || '',
      meta: { kind: 'wht' }
    }
  : null;

const normalizedLines = whtLine ? [...businessLines, whtLine] : businessLines;

const out = {
  version: '1.0',
  step: 'match_finalize',
  ok: true,
  kind,
  status,
  input_refs: {
    parse_result: args.parsed,
    local_bill_rules: args['bill-rules'],
    live_bill_rules: args['live-rules'] || null,
  },
  bill_rules_source,
  kind_rule_hit: parsed.kind_rule_hit || null,
  needs_user_kind_confirmation: needsKindConfirm,
  fields,
  interaction: {
    round: Number(userDecisions.round || 1),
    asked: userDecisions.asked || unresolved,
    confirmed: userDecisions.confirmed || [],
    unresolved,
  },
  confidence: {
    score: Number(billConfidenceScore.toFixed(4)),
    level: billConfidenceLevel,
    formula: confidenceCfg.formula || 'weighted_geometric_mean_with_missing_penalty',
    score_base: Number(scoreBase.toFixed(4)),
    missing_key_fields: keyMissing,
    penalty: Number(penalty.toFixed(4)),
    breakdown: Object.fromEntries(weightedParts.map(([k, s]) => [k, { weight: scoreWeights[k], score: Number(clamp01(s).toFixed(4)) }]))
  },
  warnings: unresolved.length ? [`unresolved_fields:${unresolved.join(',')}`] : [],
  errors: [],
  collector_payload: {
    kind,
    draft: {
      client_ref: fields.client_ref.value,
      bill_no: fields.bill_no.value,
      bill_date: fields.bill_date.value,
      due_date: fields.due_date.value,
      vendor_ref_text: fields.vendor_ref_text.value,
      vendor_ref_id: fields.vendor_ref_text.ref_id || '',
      location_ref_text: (fields.location_ref_text.value || (fields.location_ref_text.use_collector_default_when_empty ? '__USE_DEFAULT_LOCATION__' : '')),
      location_ref_id: fields.location_ref_text.value ? (fields.location_ref_text.ref_id || '') : '',
      lines: normalizedLines,
      wht: {
        rate: fields.withholding_tax?.rate ?? '',
        amount: resolvedWhtAmount ?? ''
      },
      withholding_tax: fields.withholding_tax
        ? {
            ...fields.withholding_tax,
            amount: resolvedWhtAmount ?? (fields.withholding_tax?.amount ?? ''),
          }
        : fields.withholding_tax,
      ai_confidence: {
        score: Number(billConfidenceScore.toFixed(4)),
        level: billConfidenceLevel
      }
    },
  },
  ready_to_upload: status === 'ready' || status === 'ready_with_blanks',
};

ensureDir(args.out);
fs.writeFileSync(args.out, JSON.stringify(out, null, 2), 'utf8');
console.log(JSON.stringify({ ok: true, out: args.out, status: out.status, kind: out.kind }, null, 2));
