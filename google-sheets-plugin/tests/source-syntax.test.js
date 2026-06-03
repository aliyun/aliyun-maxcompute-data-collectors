const fs = require('node:fs');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const test = require('node:test');

const GAS_SOURCE_FILES = [
  'src/Code.js',
  'src/Config.js',
  'src/OdpsSigner.js',
  'src/SettingsParser.js',
  'src/SqlExecutor.js',
  'src/TableBrowser.js',
  'src/Test.js'
];

for (const file of GAS_SOURCE_FILES) {
  test(`${file} parses as Apps Script V8 JavaScript`, () => {
    const source = fs.readFileSync(file, 'utf8');
    new vm.Script(source, { filename: file });
  });
}

test('low-level MaxCompute helpers are private Apps Script functions', () => {
  const forbiddenPublicFunctions = [
    'executeSqlQuery',
    'listSchemas',
    'listTables',
    'getTableSchema',
    'listPartitions'
  ];

  const source = GAS_SOURCE_FILES
    .map((file) => fs.readFileSync(file, 'utf8'))
    .join('\n');

  for (const name of forbiddenPublicFunctions) {
    assert.doesNotMatch(source, new RegExp(`function\\s+${name}\\s*\\(`));
    assert.match(source, new RegExp(`function\\s+${name}_\\s*\\(`));
  }
});
