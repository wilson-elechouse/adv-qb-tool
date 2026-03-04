import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { parseArgs, readJson } from './_client.mjs';

const args = parseArgs(process.argv);
if (String(args['via-workflow'] || '').toLowerCase() !== 'true') {
  throw new Error('workflow_entry_required: build_match_batch must be called via run_workflow.mjs');
}
if (!args.parsed || !args['bill-rules'] || !args.outDir) {
  console.error('usage: node scripts/build_match_batch.mjs --parsed <parse_result.json> --bill-rules <bill-rules.json> --outDir <dir> [--chunk-size 10] [--continue-on-unresolved true] --via-workflow true');
  process.exit(1);
}

const chunkSize = Math.max(1, Number(args['chunk-size'] || 10));
const parsed = readJson(args.parsed);
const records = parsed.records || [];
if (!records.length) {
  console.error('no_records: parsed.records is empty');
  process.exit(1);
}

fs.mkdirSync(args.outDir, { recursive: true });
const chunks = [];
for (let i = 0; i < records.length; i += chunkSize) {
  chunks.push(records.slice(i, i + chunkSize));
}

const results = [];
for (let cidx = 0; cidx < chunks.length; cidx += 1) {
  const chunk = chunks[cidx];
  for (let ridx = 0; ridx < chunk.length; ridx += 1) {
    const rec = chunk[ridx];
    const tempParsedPath = path.join(args.outDir, `parsed.chunk${cidx + 1}.row${ridx + 1}.json`);
    const tempMatchPath = path.join(args.outDir, `match.chunk${cidx + 1}.row${ridx + 1}.json`);

    const one = {
      ...parsed,
      recap: rec.recap,
      missing_required: rec.missing_required || [],
      records: undefined,
    };
    fs.writeFileSync(tempParsedPath, JSON.stringify(one, null, 2), 'utf8');

    const cmdArgs = [
      'skills/adv-qbo-tool/scripts/build_match_result.mjs',
      '--parsed', tempParsedPath,
      '--bill-rules', args['bill-rules'],
      '--out', tempMatchPath,
      '--via-workflow', 'true'
    ];
    if (String(args['continue-on-unresolved'] || '').toLowerCase() === 'true') {
      cmdArgs.push('--continue-on-unresolved', 'true');
    }

    const run = spawnSync('node', cmdArgs, { encoding: 'utf8' });
    if (run.status !== 0) {
      console.error(run.stderr || run.stdout || `build_match_result_failed chunk=${cidx + 1} row=${ridx + 1}`);
      process.exit(run.status || 1);
    }

    const out = readJson(tempMatchPath);
    results.push({
      chunk_index: cidx,
      row_in_chunk: ridx,
      record_index: rec.record_index,
      match_file: tempMatchPath,
      status: out.status,
      kind: out.kind,
      unresolved: out?.interaction?.unresolved || [],
    });
  }
}

const summary = {
  ok: true,
  total_records: records.length,
  chunk_size: chunkSize,
  total_chunks: chunks.length,
  results,
};
const summaryPath = path.join(args.outDir, 'batch_match_summary.json');
fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2), 'utf8');
console.log(JSON.stringify({ ok: true, summary: summaryPath, total_records: records.length, total_chunks: chunks.length }, null, 2));
