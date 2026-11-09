'use strict';
const path = require('node:path');
const {spawnSync} = require('node:child_process');
const root = path.resolve(__dirname,'..');
const files = [
  'browser-extension/slot-layout.test.js', 'browser-extension/background.test.js', 'browser-extension/content.test.js',
  'web/health.test.cjs', 'web/history.test.cjs', 'web/latency.test.cjs', 'web/trading.test.cjs', 'web/system.test.cjs',
];
for (const file of files) {
  const result = spawnSync(process.execPath,[path.join(root,file)],{cwd:root,stdio:'inherit'});
  if (result.error || result.status !== 0) { console.error('Failed:',file,result.error?.message||result.status); process.exit(1); }
}
console.log('All offline JavaScript suites passed. No live browser/backend was controlled.');
