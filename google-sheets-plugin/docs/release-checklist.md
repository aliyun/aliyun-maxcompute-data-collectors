# Release checklist

This repository builds an Editor add-on. A local pass does not authorize a deployment or certify OAuth approval.

1. Run `npm run release:local` on a clean candidate commit. Archive the complete output and SHA-256 of the generated package. `Test.js` must remain excluded.
2. Attach the script to a standard Google Cloud project; configure the OAuth consent screen for that project.
3. Requested OAuth scopes match in all three places: `src/appsscript.json`, OAuth consent configuration, and Marketplace SDK. There are no broad Drive scopes. The full-spreadsheet scope is required by the existing scheduler's `SpreadsheetApp.openById`; `script.scriptapp` permits installation/removal of the user's time trigger. This scope rationale must be reviewed for the target distribution. Do not silently downgrade to `currentonly` while retaining scheduling.
4. `https://www.googleapis.com/auth/userinfo.email` supports submitter attribution. Review actual scopes against [the manifest and data disclosure](marketplace-submission-draft.md).
5. Create an immutable Apps Script version. The Marketplace SDK listing points to that exact Apps Script version number. CardService deployment IDs are for CardService-based Google Workspace add-ons, and are not the Editor add-on version field.
6. Install an Editor add-on test deployment into a QA spreadsheet. Complete [external-qa-evidence-template.md](external-qa-evidence-template.md), including real MaxCompute, cancel, Attach, scheduler, and OSS checks. Keep credentials, query result data, and private URLs out of public evidence.
7. Complete OAuth verification and Marketplace app review when required. Record approved visibility and owner release authorization. Run `npm run release:verify-public` against the completed evidence. The blank template must fail this command.

The production fetch allowlist includes explicit MaxCompute region hosts, Google userinfo, and bucket subdomains of the 20 OSS regional hosts offered by Settings. It does not permit arbitrary `aliyuncs.com` hosts. A custom region needs a reviewed manifest update before a versioned deployment. Test deployments do not prove the versioned allowlist works.

## Official Google References

- [Publishing overview](https://developers.google.com/workspace/add-ons/how-tos/publish-add-on-overview): use the Apps Script version number for an Editor add-on.
- [Testing Editor add-ons](https://developers.google.com/workspace/add-ons/how-tos/testing-editor-addons).
- [Configure OAuth consent](https://developers.google.com/workspace/guides/configure-oauth-consent-screen).
- [Marketplace SDK configuration](https://developers.google.com/workspace/marketplace/enable-configure-sdk): keep OAuth scopes aligned across the OAuth consent screen, Marketplace SDK,
  and Apps Script manifest.
- [App review](https://developers.google.com/workspace/marketplace/about-app-review).
- [Fetch URL allowlists](https://developers.google.com/apps-script/manifest/allowlist-url): HTTPS prefixes require a path; a single leading wildcard can match bucket subdomains. Exact regional suffixes remain constrained.
