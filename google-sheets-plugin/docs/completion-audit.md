# Completion audit

## Concrete Success Criteria

Local package tests, real Apps Script execution, real MaxCompute read-only/audit behavior, and target-visibility OAuth/Marketplace approval are separate acceptance criteria. This work is not complete for public Marketplace release.

## Prompt-To-Artifact Checklist

- SQL guard and credentials: server and browser tests under `tests/`.
- User workflow: `src/Sidebar.html`, with offline callbacks and rendering tests.
- Production package: explicit allowlists in `scripts/build-release.js` and `scripts/verify-release-package.js`.
- External deployment and approval: [external-qa-evidence-template.md](external-qa-evidence-template.md) and [release-checklist.md](release-checklist.md).

## Latest Local Evidence

Run `npm run release:local` for the exact commit under review and retain its complete log. A local result covers mocks and package structure only; no Google project, spreadsheet, OSS object, or Marketplace listing is created by those tests.

## Remaining Required Evidence

The external evidence template is deliberately TODO and `release:verify-public` must reject it. Real Apps Script installation, OAuth consent/scopes, MaxCompute query/cancel/Attach/audit checks, scheduler trigger lifecycle, OSS upload verification, and owner approval are outstanding. Do not replace TODO with generated PASS values.
