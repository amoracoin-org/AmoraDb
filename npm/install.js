'use strict';

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const platform = process.platform;
const arch = process.arch;

const prebuilt = path.join(__dirname, 'prebuilds', `${platform}-${arch}`, 'amoradb.node');
if (fs.existsSync(prebuilt)) {
  process.exit(0);
}

if (process.env.AMORADB_SKIP_BUILD === '1') {
  process.exit(0);
}

const nodeGypBin = (() => {
  try {
    return require.resolve('node-gyp/bin/node-gyp.js');
  } catch (_) {
    return null;
  }
})();

if (!nodeGypBin) {
  process.stderr.write('AmoraDB: no prebuilt binary and node-gyp is not available.\n');
  process.stderr.write('Install a build toolchain (python + compiler) or use a supported platform prebuild.\n');
  process.exit(1);
}

const result = spawnSync(process.execPath, [nodeGypBin, 'rebuild'], {
  cwd: __dirname,
  stdio: 'inherit',
});

process.exit(result.status == null ? 1 : result.status);
