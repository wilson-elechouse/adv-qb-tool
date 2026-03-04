import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { parseArgs } from './_client.mjs';

function run(cmd, args, cwd) {
  const r = spawnSync(cmd, args, { cwd, encoding: 'utf8' });
  if (r.status !== 0) throw new Error((r.stderr || r.stdout || `${cmd} failed`).trim());
  return (r.stdout || '').trim();
}

function exists(p) { return fs.existsSync(p); }
function readJson(p) { return JSON.parse(fs.readFileSync(p, 'utf8')); }
function writeJson(p, obj) { fs.mkdirSync(path.dirname(p), { recursive: true }); fs.writeFileSync(p, JSON.stringify(obj, null, 2), 'utf8'); }

const args = parseArgs(process.argv);
throw new Error('entry_removed: use python skills/adv-qbo-tool/scripts/workflow.py as the only workflow entry');

const workdir = path.resolve(args.dir);
fs.mkdirSync(workdir, { recursive: true });
const statePath = path.join(workdir, 'workflow_state.json');

let state = {
  state: 'S1_PARSE_IDENTIFY',
  inputs: { file: path.resolve(args.file), bill_rules: path.resolve(args['bill-rules']), config: args.config ? path.resolve(args.config) : null },
  artifacts: {},
  flags: { confirmed: String(args.confirmed || 'false') === 'true' },
  error: null
};

try {
  // S1
  const parseOut = path.join(workdir, 'parse_result.json');
  run('python', ['skills/adv-qbo-tool/scripts/parse_payment_request_xlsx.py', '--file', state.inputs.file, '--out', parseOut], process.cwd());
  if (!exists(parseOut)) throw new Error('parse_result_missing');
  const parsed = readJson(parseOut);
  if (!parsed.ok) throw new Error(`parse_failed:${parsed.error || 'unknown'}`);
  state.artifacts.parse_result = parseOut;
  state.state = 'S2_MATCH_BUILD';
  writeJson(statePath, state);

  // S2
  const records = parsed.records || [];
  if (records.length > 1) {
    const outDir = path.join(workdir, 'batch');
    run('node', ['skills/adv-qbo-tool/scripts/build_match_batch.mjs', '--parsed', parseOut, '--bill-rules', state.inputs.bill_rules, '--outDir', outDir, '--chunk-size', String(args['chunk-size'] || 10), '--via-workflow', 'true'], process.cwd());
    const summary = path.join(outDir, 'batch_match_summary.json');
    if (!exists(summary)) throw new Error('batch_match_summary_missing');
    state.artifacts.batch_match_summary = summary;
    state.state = 'S3_CONFIRM_RENDER';
    writeJson(statePath, state);

    // render first record for confirmation preview
    const s = readJson(summary);
    const firstMatch = s.results?.[0]?.match_file;
    if (!firstMatch) throw new Error('batch_first_match_missing');
    const recapOut = path.join(workdir, 'confirmation_recap.json');
    const txt = run('node', ['skills/adv-qbo-tool/scripts/render_confirmation_recap.mjs', '--match-result', firstMatch, '--state-file', statePath, '--via-workflow', 'true'], process.cwd());
    fs.writeFileSync(recapOut, txt, 'utf8');
    state.artifacts.confirmation_recap = recapOut;
    state.artifacts.first_match_result = firstMatch;
  } else {
    const matchOut = path.join(workdir, 'match_result.json');
    run('node', ['skills/adv-qbo-tool/scripts/build_match_result.mjs', '--parsed', parseOut, '--bill-rules', state.inputs.bill_rules, '--out', matchOut, '--via-workflow', 'true'], process.cwd());
    if (!exists(matchOut)) throw new Error('match_result_missing');
    state.artifacts.match_result = matchOut;
    state.state = 'S3_CONFIRM_RENDER';
    writeJson(statePath, state);

    const recapOut = path.join(workdir, 'confirmation_recap.json');
    const txt = run('node', ['skills/adv-qbo-tool/scripts/render_confirmation_recap.mjs', '--match-result', matchOut, '--state-file', statePath, '--via-workflow', 'true'], process.cwd());
    fs.writeFileSync(recapOut, txt, 'utf8');
    state.artifacts.confirmation_recap = recapOut;
  }

  // S3 -> S4 gate
  state.state = 'S4_UPLOAD';
  writeJson(statePath, state);

  if (!state.flags.confirmed) {
    state.state = 'WAIT_CONFIRMATION';
    writeJson(statePath, state);
    console.log(JSON.stringify({ ok: true, state: state.state, state_file: statePath, recap: state.artifacts.confirmation_recap }, null, 2));
    process.exit(0);
  }

  if (!state.inputs.config) throw new Error('upload_config_required_when_confirmed');
  if (!state.artifacts.match_result) {
    state.state = 'WAIT_CONFIRMATION';
    writeJson(statePath, state);
    console.log(JSON.stringify({ ok: true, state: state.state, note: 'batch mode requires per-record confirmation/upload orchestration', state_file: statePath }, null, 2));
    process.exit(0);
  }

  // S4
  const submitOut = run('node', ['skills/adv-qbo-tool/scripts/submit_from_match_result.mjs', '--config', state.inputs.config, '--match-result', state.artifacts.match_result, '--confirmation-received', 'true', '--via-workflow', 'true'], process.cwd());
  const submitPath = path.join(workdir, 'submit_result.json');
  fs.writeFileSync(submitPath, submitOut, 'utf8');
  state.artifacts.submit_result = submitPath;
  state.state = 'S5_DONE';
  writeJson(statePath, state);

  console.log(JSON.stringify({ ok: true, state: state.state, state_file: statePath, submit_result: submitPath }, null, 2));
} catch (e) {
  state.state = 'ERROR';
  state.error = String(e?.message || e);
  writeJson(statePath, state);
  console.error(state.error);
  process.exit(1);
}
