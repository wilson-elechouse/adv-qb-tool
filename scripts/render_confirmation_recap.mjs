import { parseArgs, readJson } from './_client.mjs';

const args = parseArgs(process.argv);
if (String(args['via-workflow'] || '').toLowerCase() !== 'true') {
  throw new Error('workflow_entry_required: confirmation recap must be called via run_workflow.mjs');
}
if (!args['state-file']) {
  throw new Error('workflow_proof_missing: pass --state-file <workflow_state.json>');
}
if (!args['match-result']) {
  throw new Error('match_result_required: pass --match-result <match_result.json>');
}
const wf = readJson(args['state-file']);
const allowedStates = new Set(['S3_CONFIRM_RENDER', 'S4_UPLOAD', 'WAIT_CONFIRMATION']);
if (!allowedStates.has(String(wf?.state || ''))) {
  throw new Error(`workflow_state_invalid_for_recap:${wf?.state || 'unknown'}`);
}
const mrPath = String(args['match-result']);
let proofOk = false;
const expectedMatch = wf?.artifacts?.match_result || wf?.artifacts?.first_match_result || '';
if (expectedMatch && String(expectedMatch) === mrPath) {
  proofOk = true;
}
if (!proofOk && wf?.artifacts?.batch_match_summary) {
  const summary = readJson(wf.artifacts.batch_match_summary);
  const allowed = new Set((summary?.results || []).map((x) => String(x.match_file || '')));
  if (allowed.has(mrPath)) proofOk = true;
}
if (!proofOk) {
  throw new Error('workflow_proof_mismatch: match-result is not bound to workflow_state artifacts');
}
const mr = readJson(args['match-result']);
if (!mr?.ok) throw new Error('match_result_invalid: ok=false');
if (!mr.fields) throw new Error('match_result_invalid: fields missing');

const needSuggestFields = ['vendor_ref_text', 'category_ref_text', 'location_ref_text'];
for (const k of needSuggestFields) {
  const f = mr.fields[k] || {};
  if (f.needs_user_confirmation) {
    const hasAlternatives = Array.isArray(f.alternatives) && f.alternatives.length > 0;
    const unavailable = String(mr.bill_rules_source || '') === 'unavailable';
    if (!hasAlternatives && !unavailable) {
      throw new Error(`suggestion_render_incomplete:${k}`);
    }
  }
}

const out = {
  confirmation_required: true,
  kind: mr.kind,
  bill_rules_source: mr.bill_rules_source || 'unknown',
  recap: {
    vendor: {
      final_value: mr.fields.vendor_ref_text?.value || '',
      suggested_value: mr.fields.vendor_ref_text?.value || '',
      confidence: mr.fields.vendor_ref_text?.confidence ?? 0,
      top3: mr.fields.vendor_ref_text?.alternatives || [],
      needs_user_confirmation: !!mr.fields.vendor_ref_text?.needs_user_confirmation,
    },
    bill_number: mr.fields.bill_no?.value || '',
    request_no: mr.fields.client_ref?.value || '',
    bill_date: mr.fields.bill_date?.value || '',
    due_date: mr.fields.due_date?.value || '',
    location: {
      final_value: mr.fields.location_ref_text?.value || '',
      suggested_value: mr.fields.location_ref_text?.value || '',
      confidence: mr.fields.location_ref_text?.confidence ?? 0,
      top3: mr.fields.location_ref_text?.alternatives || [],
      needs_user_confirmation: !!mr.fields.location_ref_text?.needs_user_confirmation,
      fallback: mr.fields.location_ref_text?.fallback || 'collector_default_if_empty',
      use_collector_default_when_empty: !!mr.fields.location_ref_text?.use_collector_default_when_empty
    },
    category: {
      final_value: mr.fields.category_ref_text?.value || '',
      suggested_value: mr.fields.category_ref_text?.value || '',
      confidence: mr.fields.category_ref_text?.confidence ?? 0,
      top3: mr.fields.category_ref_text?.alternatives || [],
      needs_user_confirmation: !!mr.fields.category_ref_text?.needs_user_confirmation,
    },
    class_ref: mr.collector_payload?.draft?.lines?.[0]?.class_ref_text || '',
    tax: {
      final_value: mr.fields.tax_code_ref_text?.value || '',
      suggested_value: mr.fields.tax_code_ref_text?.value || '',
      confidence: mr.fields.tax_code_ref_text?.confidence ?? 0,
      top3: mr.fields.tax_code_ref_text?.alternatives || [],
      needs_user_confirmation: !!mr.fields.tax_code_ref_text?.needs_user_confirmation,
    },
    withholding_tax: mr.fields.withholding_tax || {},
    confidence: mr.confidence || null,
    unresolved: mr.interaction?.unresolved || []
  },
  next_action: 'wait_for_user_confirmation'
};

console.log(JSON.stringify(out, null, 2));
