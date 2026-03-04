import { spawnSync } from 'node:child_process';

function run(args) {
  return spawnSync('node', args, { encoding: 'utf8' });
}

const bad = run(['skills/adv-qbo-tool/scripts/render_confirmation_recap.mjs', '--match-result', 'skills/adv-qbo-tool/references/samples/bad_match_result.json']);
if (bad.status === 0) {
  console.error('expected failure for bad_match_result');
  process.exit(1);
}

const ok = run(['skills/adv-qbo-tool/scripts/render_confirmation_recap.mjs', '--match-result', 'tmp/adv-qbo/match_result.round4.json']);
if (ok.status !== 0) {
  console.error('expected success for good match_result');
  console.error(ok.stderr || ok.stdout);
  process.exit(1);
}

console.log('render guard test passed');
