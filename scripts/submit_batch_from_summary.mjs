import fs from 'node:fs';
import path from 'node:path';
import { parseArgs, readJson } from './_client.mjs';
import { spawnSync } from 'node:child_process';

function runNode(args) {
  const r = spawnSync('node', args, { encoding: 'utf8' });
  if (r.status !== 0) throw new Error((r.stderr || r.stdout || 'submit failed').trim());
  return JSON.parse((r.stdout || '{}').trim());
}

const args = parseArgs(process.argv);
if (!args.summary || !args.config) {
  throw new Error('usage: node scripts/submit_batch_from_summary.mjs --summary <batch_match_summary.json> --config <company.json> --confirmation-received true --via-workflow true');
}
if (String(args['confirmation-received'] || '').toLowerCase() !== 'true') {
  throw new Error('confirmation_required');
}
if (String(args['via-workflow'] || '').toLowerCase() !== 'true') {
  throw new Error('workflow_entry_required');
}

const summary = readJson(args.summary);
const out = { ok: true, total: 0, success: 0, fail: 0, results: [] };
for (const item of (summary.results || [])) {
  out.total += 1;
  try {
    const res = runNode([
      'skills/adv-qbo-tool/scripts/submit_from_match_result.mjs',
      '--config', args.config,
      '--match-result', item.match_file,
      '--confirmation-received', 'true',
      '--via-workflow', 'true'
    ]);
    out.success += 1;
    out.results.push({ record_index: item.record_index, ok: true, submission_id: res.submission_id, view_url: res.view_url });
  } catch (e) {
    out.fail += 1;
    out.results.push({ record_index: item.record_index, ok: false, error: String(e?.message || e) });
  }
}

const outputPath = path.join(path.dirname(args.summary), 'batch_submit_result.json');
fs.writeFileSync(outputPath, JSON.stringify(out, null, 2), 'utf8');
console.log(JSON.stringify({ ok: true, out: outputPath, success: out.success, fail: out.fail }, null, 2));
