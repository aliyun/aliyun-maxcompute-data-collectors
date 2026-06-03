const fs = require('node:fs');
const path = require('node:path');

const DEFAULT_SOURCE_DIR = path.resolve(__dirname, '..', 'src');
const DEFAULT_OUTPUT_DIR = path.resolve(__dirname, '..', 'dist', 'apps-script');

const RELEASE_FILES = [
  {
    source: '.clasp.json',
    fallbackSource: '.clasp.example.json',
    target: '.clasp.json'
  },
  'Code.js',
  'Config.js',
  'OdpsSigner.js',
  'OssExporter.js',
  'OssSigner.js',
  'Scheduler.js',
  'SettingsParser.js',
  'Settings.html',
  'Sidebar.html',
  'SqlExecutor.js',
  'TableBrowser.js',
  'appsscript.json'
];

const QA_ONLY_FILES = [
  'Test.js'
];

function buildRelease(options = {}) {
  const sourceDir = options.sourceDir || DEFAULT_SOURCE_DIR;
  const outputDir = options.outputDir || DEFAULT_OUTPUT_DIR;

  if (!fs.existsSync(sourceDir)) {
    throw new Error(`Source directory does not exist: ${sourceDir}`);
  }

  fs.rmSync(outputDir, { recursive: true, force: true });
  fs.mkdirSync(outputDir, { recursive: true });

  const copied = [];
  for (const entry of RELEASE_FILES) {
    const file = typeof entry === 'string' ? entry : entry.target;
    const sourceNames = typeof entry === 'string'
      ? [entry]
      : [entry.source, entry.fallbackSource].filter(Boolean);
    const sourceName = sourceNames.find((candidate) => fs.existsSync(path.join(sourceDir, candidate)));
    if (!sourceName) {
      throw new Error(`Required release source file is missing: ${sourceNames.join(' or ')}`);
    }
    const sourcePath = path.join(sourceDir, sourceName);
    if (!fs.existsSync(sourcePath)) {
      throw new Error(`Required release source file is missing: ${file}`);
    }
    const outputPath = path.join(outputDir, file);
    fs.copyFileSync(sourcePath, outputPath);
    copied.push(file);
  }

  return {
    sourceDir,
    outputDir,
    copied: copied.sort(),
    excluded: QA_ONLY_FILES.slice().sort()
  };
}

if (require.main === module) {
  const result = buildRelease();
  console.log(`release build: ${path.relative(process.cwd(), result.outputDir)}`);
  console.log(`copied: ${result.copied.join(', ')}`);
  console.log(`excluded: ${result.excluded.join(', ')}`);
}

module.exports = {
  buildRelease
};
