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

function ensureArray(v) {
  if (Array.isArray(v)) return v;
  throw new Error('batch_input_must_be_array');
}

function normalizeKind(item) {
  const k = String(item?.kind || '').trim().toLowerCase();
  if (k === 'bill' || k === 'bill_payment') return k;
  throw new Error(`invalid_kind:${k || 'empty'}`);
}

function hasText(v) {
  return typeof v === 'string' ? v.trim().length > 0 : v !== null && v !== undefined;
}

function validateInlinePayload(kind, payload = {}) {
  const issues = [];
  if (kind === 'bill') {
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
  }
  if (kind === 'bill_payment') {
    if (!hasText(payload.client_ref)) issues.push('missing_client_ref');
    if (!hasText(payload.pay_date)) issues.push('missing_pay_date');
  }
  return issues;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const args = parseArgs(process.argv);
const configPath = args.config;
const inputPath = args.input;
const continueOnError = String(args['continue-on-error'] || 'true') === 'true';
const finalFlag = String(args.final || 'false');
const mappingConfirmed = String(args['mapping-confirmed'] || 'false') === 'true';
const confirmationReceived = String(args['confirmation-received'] || '').toLowerCase() === 'true';
const allowSample = String(args['allow-sample'] || 'false').toLowerCase() === 'true';

if (!configPath) throw new Error('--config is required');
if (!inputPath) throw new Error('--input is required');
if (!confirmationReceived) throw new Error('confirmation_required: pass --confirmation-received true only after explicit user approval');

const input = readJson(inputPath);
const items = ensureArray(input);

const preflight = {
  ok: true,
  error: null,
  checklist: [],
  item_issues: []
};
if (!mappingConfirmed) {
  preflight.ok = false;
  preflight.error = 'mapping_confirmation_required';
  preflight.checklist = [
    'confirm_source_columns_to_target_fields',
    'confirm_vendor_mapping_rule',
    'confirm_account_mapping_rule',
    'confirm_tax_and_class_mapping_rule',
    'confirm_date_rule_and_conflict_handling'
  ];
}
for (let i = 0; i < items.length; i += 1) {
  const item = items[i] || {};
  const kind = String(item?.kind || '').trim().toLowerCase();
  if (item.payload && typeof item.payload === 'object') {
    const issues = validateInlinePayload(kind, item.payload);
    if (issues.length) preflight.item_issues.push({ index: i + 1, kind, issues });
  }
}
if (preflight.item_issues.length > 0) {
  preflight.ok = false;
  preflight.error = preflight.error || 'preflight_failed_user_confirmation_required';
}
if (!preflight.ok) {
  console.log(JSON.stringify({
    ok: false,
    error: preflight.error,
    message: 'User confirmation required before batch creation.',
    preflight
  }, null, 2));
  process.exit(2);
}

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'adv-qbo-batch-'));
const summary = {
  mode: continueOnError ? 'continue-on-error' : 'stop-on-error',
  total: items.length,
  ok: 0,
  failed: 0,
  failed_by_reason: {},
  results: []
};

for (let i = 0; i < items.length; i += 1) {
  const item = items[i] || {};
  let payloadPath = '';
  try {
    const kind = normalizeKind(item);
    if (item.payloadPath) {
      payloadPath = path.isAbsolute(item.payloadPath)
        ? item.payloadPath
        : path.resolve(path.dirname(inputPath), item.payloadPath);
      if (!allowSample && String(payloadPath).includes(`${path.sep}references${path.sep}samples${path.sep}`)) {
        throw new Error('sample_payload_blocked: do not use references/samples in real runs');
      }
    } else if (item.payload && typeof item.payload === 'object') {
      payloadPath = path.join(tmpDir, `payload-${i + 1}.json`);
      fs.writeFileSync(payloadPath, JSON.stringify(item.payload, null, 2));
    } else {
      throw new Error('missing_payload_or_payloadPath');
    }

    const scriptName = kind === 'bill' ? 'submit_bill.mjs' : 'submit_billpayment.mjs';
    const scriptPath = path.resolve(__dirname, scriptName);

    const run = await runNodeScript(scriptPath, [
      '--config', configPath,
      '--payload', payloadPath,
      '--final', String(item.final ?? finalFlag),
      '--confirmation-received', 'true',
      ...(allowSample ? ['--allow-sample', 'true'] : [])
    ]);

    if (run.code !== 0 || !run.json) {
      throw new Error((run.stderr || run.stdout || `script_failed_${run.code}`).trim());
    }

    summary.ok += 1;
    summary.results.push({ index: i + 1, kind, ok: true, output: run.json });
  } catch (e) {
    summary.failed += 1;
    const reason = String(e?.message || e);
    summary.failed_by_reason[reason] = (summary.failed_by_reason[reason] || 0) + 1;
    summary.results.push({ index: i + 1, ok: false, error: reason });
    if (!continueOnError) break;
  }
}

try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}

console.log(JSON.stringify(summary, null, 2));
