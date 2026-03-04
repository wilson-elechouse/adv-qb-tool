import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { parseArgs, readJson, createSession } from './_client.mjs';

function sha256(text) {
  return crypto.createHash('sha256').update(text, 'utf8').digest('hex');
}

function normTaxLabels(raw) {
  const q = (raw?.rules || raw)?.qboOptionDictionaries || {};
  const arr = (q.taxCodes || []).map((x) => (x?.label || x?.key || String(x || ''))).filter(Boolean).map((x) => String(x).toLowerCase());
  const s = arr.join(' | ');
  return {
    wht_out_of_scope_found: /wht\s*-\s*out\s*of\s*scope/.test(s),
    vat_12_found: /vat\s*12%/.test(s),
    non_taxable_found: /non-?taxable/.test(s),
  };
}

const args = parseArgs(process.argv);
if (!args.config || !args['out-dir']) {
  throw new Error('usage: node scripts/refresh_bill_rules.mjs --config <company.json> --out-dir <rules_cache_dir> [--set-latest true] [--dry-run true] [--tenant-id <id>]');
}

const cfg = readJson(args.config);
if (args['tenant-id']) cfg.tenant_id = args['tenant-id'];
const base = cfg.base_url || 'https://qb.uudc.us';
const outDir = path.resolve(args['out-dir']);
const setLatest = String(args['set-latest'] || 'true').toLowerCase() === 'true';
const dryRun = String(args['dry-run'] || 'false').toLowerCase() === 'true';

fs.mkdirSync(outDir, { recursive: true });
const latestPath = path.join(outDir, 'latest.json');
const manifestPath = path.join(outDir, 'manifest.json');

const api = await createSession(base, cfg);
const rules = await api.getJson('/api/bill-rules');
const payload = JSON.stringify(rules, null, 2);
const newHash = sha256(payload);
const oldHash = fs.existsSync(latestPath) ? sha256(fs.readFileSync(latestPath, 'utf8')) : null;
const changed = newHash !== oldHash;

if (dryRun) {
  console.log(JSON.stringify({ ok: true, mode: 'dry_run', changed, old_hash: oldHash, new_hash: newHash, tenant_id: cfg.tenant_id || null, base_url: base }, null, 2));
  process.exit(0);
}

const stamp = new Date().toISOString().replace(/[-:]/g, '').slice(0, 15).replace('T', '-');
const snapPath = path.join(outDir, `bill-rules.${stamp}.json`);
fs.writeFileSync(snapPath, payload, 'utf8');
if (setLatest) fs.writeFileSync(latestPath, payload, 'utf8');

const manifest = {
  updated_at: new Date().toISOString(),
  tenant_id: cfg.tenant_id || null,
  base_url: base,
  latest_snapshot: snapPath,
  latest_hash: newHash,
  previous_hash: oldHash,
  changed,
  set_latest: setLatest,
  source: 'manual_refresh',
  taxonomy_check: normTaxLabels(rules),
};
fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');

console.log(JSON.stringify({ ok: true, changed, snapshot: snapPath, latest: setLatest ? latestPath : null, manifest: manifestPath, hash: newHash, taxonomy_check: manifest.taxonomy_check }, null, 2));
