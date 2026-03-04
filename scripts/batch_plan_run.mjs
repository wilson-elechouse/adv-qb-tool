import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { parseArgs, readJson } from './_client.mjs';

function runNodeScript(scriptPath, args = []) {
  return new Promise((resolve) => {
    const p = spawn(process.execPath, [scriptPath, ...args], { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    let err = '';
    p.stdout.on('data', (d) => { out += d.toString(); });
    p.stderr.on('data', (d) => { err += d.toString(); });
    p.on('close', (code) => {
      const text = (out || '').trim();
      let json = null;
      try { json = text ? JSON.parse(text) : null; } catch {}
      resolve({ code, stdout: out, stderr: err, json });
    });
  });
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function hasText(v) {
  return typeof v === 'string' ? v.trim().length > 0 : v !== null && v !== undefined;
}

function validateBillPayload(payload = {}) {
  const issues = [];
  if (!hasText(payload.client_ref)) issues.push('missing_client_ref');
  if (!hasText(payload.vendor_ref_text)) issues.push('missing_vendor_ref_text');
  if (!hasText(payload.bill_date)) issues.push('missing_bill_date');
  if (!hasText(payload.due_date)) issues.push('missing_due_date');
  if (hasText(payload.bill_date) && hasText(payload.due_date) && String(payload.bill_date) > String(payload.due_date)) {
    issues.push('due_date_before_bill_date');
  }
  if (!Array.isArray(payload.lines) || payload.lines.length === 0) {
    issues.push('missing_lines');
  } else {
    const l0 = payload.lines[0] || {};
    if (!hasText(l0.account_ref_text)) issues.push('missing_line_account_ref_text');
    if (!(Number(l0.amount) > 0)) issues.push('invalid_line_amount');
  }
  return issues;
}

function validateBillPaymentPayload(payload = {}) {
  const issues = [];
  if (!hasText(payload.client_ref)) issues.push('missing_client_ref');
  if (!hasText(payload.pay_date)) issues.push('missing_pay_date');
  const links = payload?.lines || payload?.bill_links || [];
  if (!Array.isArray(links) || links.length === 0) issues.push('missing_bill_links');
  return issues;
}

function runPreflight(items = [], { mappingConfirmed = false, reportPath = '' } = {}) {
  const result = {
    ok: true,
    error: null,
    checklist: [],
    issue_count: 0,
    item_issues: []
  };

  if (!mappingConfirmed) {
    result.ok = false;
    result.error = 'mapping_confirmation_required';
    result.checklist = [
      'confirm_source_columns_to_target_fields',
      'confirm_vendor_mapping_rule',
      'confirm_account_mapping_rule',
      'confirm_tax_and_class_mapping_rule',
      'confirm_date_rule_and_conflict_handling'
    ];
  }

  for (let i = 0; i < items.length; i += 1) {
    const item = items[i] || {};
    const kind = String(item.kind || '').toLowerCase();
    const payload = item.payload && typeof item.payload === 'object' ? item.payload : null;
    if (!payload) {
      result.item_issues.push({ index: i + 1, kind: kind || 'unknown', issues: ['missing_inline_payload_for_preflight'] });
      continue;
    }
    const issues = kind === 'bill'
      ? validateBillPayload(payload)
      : (kind === 'bill_payment' ? validateBillPaymentPayload(payload) : ['invalid_kind']);
    if (issues.length) result.item_issues.push({ index: i + 1, kind, issues });
  }

  result.issue_count = result.item_issues.reduce((n, x) => n + x.issues.length, 0);
  if (result.item_issues.length > 0) {
    result.ok = false;
    result.error = result.error || 'preflight_failed_user_confirmation_required';
  }

  if (reportPath) fs.writeFileSync(reportPath, JSON.stringify(result, null, 2));
  return result;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const args = parseArgs(process.argv);
const configPath = args.config;
const inputPath = args.input;
const continueOnError = String(args['continue-on-error'] || 'true') === 'true';
const chunkSize = Math.max(1, Number(args['chunk-size'] || 50));
const pauseMs = Math.max(0, Number(args['pause-ms'] || 1500));
const maxItemsPerJob = Math.max(1, Number(args['max-items'] || 500));
const resume = String(args.resume || 'false') === 'true';
const mappingConfirmed = String(args['mapping-confirmed'] || 'false') === 'true';
const confirmationReceived = String(args['confirmation-received'] || '').toLowerCase() === 'true';
const allowSample = String(args['allow-sample'] || 'false').toLowerCase() === 'true';

if (!configPath) throw new Error('--config is required');
if (!inputPath) throw new Error('--input is required');
if (!confirmationReceived) throw new Error('confirmation_required: pass --confirmation-received true only after explicit user approval');

const inputAbs = path.isAbsolute(inputPath) ? inputPath : path.resolve(process.cwd(), inputPath);
const allItems = readJson(inputAbs);
if (!Array.isArray(allItems)) throw new Error('batch_input_must_be_array');
if (allItems.length > maxItemsPerJob) {
  throw new Error(`too_many_items:${allItems.length};max_items=${maxItemsPerJob}`);
}

const preflightReportPath = args['preflight-report']
  ? (path.isAbsolute(args['preflight-report']) ? args['preflight-report'] : path.resolve(process.cwd(), args['preflight-report']))
  : `${inputAbs}.preflight.json`;
const preflight = runPreflight(allItems, { mappingConfirmed, reportPath: preflightReportPath });
if (!preflight.ok) {
  console.log(JSON.stringify({
    ok: false,
    error: preflight.error,
    message: 'User confirmation required before batch creation. Review preflight report.',
    preflight_report: preflightReportPath,
    preflight
  }, null, 2));
  process.exit(2);
}

const checkpointPath = args.checkpoint
  ? (path.isAbsolute(args.checkpoint) ? args.checkpoint : path.resolve(process.cwd(), args.checkpoint))
  : `${inputAbs}.checkpoint.json`;

const chunks = [];
for (let i = 0; i < allItems.length; i += chunkSize) {
  chunks.push(allItems.slice(i, i + chunkSize));
}

const state = {
  mode: 'chunked-serial',
  input: inputAbs,
  total_items: allItems.length,
  chunk_size: chunkSize,
  total_chunks: chunks.length,
  continue_on_error: continueOnError,
  max_items: maxItemsPerJob,
  started_at: new Date().toISOString(),
  next_chunk_index: 0,
  chunk_results: [],
  ok: 0,
  failed: 0,
  failed_by_reason: {}
};

if (resume && fs.existsSync(checkpointPath)) {
  const prev = readJson(checkpointPath);
  if (prev?.input === inputAbs && Number(prev?.total_items) === allItems.length) {
    state.next_chunk_index = Number(prev.next_chunk_index || 0);
    state.chunk_results = Array.isArray(prev.chunk_results) ? prev.chunk_results : [];
    state.ok = Number(prev.ok || 0);
    state.failed = Number(prev.failed || 0);
    state.failed_by_reason = prev.failed_by_reason && typeof prev.failed_by_reason === 'object' ? prev.failed_by_reason : {};
  }
}

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'adv-qbo-plan-'));
const batchScript = path.resolve(__dirname, 'batch_run.mjs');

for (let i = state.next_chunk_index; i < chunks.length; i += 1) {
  const chunk = chunks[i];
  const normalizedChunk = chunk.map((item) => {
    const out = { ...(item || {}) };
    if (out.payloadPath && typeof out.payloadPath === 'string') {
      out.payloadPath = path.isAbsolute(out.payloadPath)
        ? out.payloadPath
        : path.resolve(path.dirname(inputAbs), out.payloadPath);
      if (!allowSample && String(out.payloadPath).includes(`${path.sep}references${path.sep}samples${path.sep}`)) {
        throw new Error('sample_payload_blocked: do not use references/samples in real runs');
      }
    }
    return out;
  });
  const chunkFile = path.join(tmpDir, `chunk-${i + 1}.json`);
  fs.writeFileSync(chunkFile, JSON.stringify(normalizedChunk, null, 2));

  const run = await runNodeScript(batchScript, [
    '--config', configPath,
    '--input', chunkFile,
    '--continue-on-error', String(continueOnError),
    '--mapping-confirmed', 'true',
    '--confirmation-received', 'true',
    ...(allowSample ? ['--allow-sample', 'true'] : [])
  ]);

  if (run.code !== 0 || !run.json) {
    const reason = (run.stderr || run.stdout || `chunk_failed_${run.code}`).trim();
    state.chunk_results.push({ chunk_index: i + 1, ok: false, error: reason });
    state.failed += chunk.length;
    state.failed_by_reason[reason] = (state.failed_by_reason[reason] || 0) + chunk.length;
    state.next_chunk_index = i + 1;
    fs.writeFileSync(checkpointPath, JSON.stringify(state, null, 2));
    if (!continueOnError) break;
  } else {
    const j = run.json;
    state.chunk_results.push({
      chunk_index: i + 1,
      ok: true,
      total: j.total,
      ok_count: j.ok,
      failed_count: j.failed,
      failed_by_reason: j.failed_by_reason || {},
      results: j.results || []
    });
    state.ok += Number(j.ok || 0);
    state.failed += Number(j.failed || 0);
    for (const [k, v] of Object.entries(j.failed_by_reason || {})) {
      state.failed_by_reason[k] = (state.failed_by_reason[k] || 0) + Number(v || 0);
    }
    state.next_chunk_index = i + 1;
    fs.writeFileSync(checkpointPath, JSON.stringify(state, null, 2));
  }

  if (i < chunks.length - 1 && pauseMs > 0) await sleep(pauseMs);
}

state.finished_at = new Date().toISOString();
state.done = state.next_chunk_index >= chunks.length;

fs.writeFileSync(checkpointPath, JSON.stringify(state, null, 2));
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}

console.log(JSON.stringify(state, null, 2));
