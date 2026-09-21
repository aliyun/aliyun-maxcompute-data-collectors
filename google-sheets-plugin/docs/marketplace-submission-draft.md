# Marketplace submission draft — not submitted

MaxCompute Connector is an Editor add-on for Google Sheets. The listing references an Apps Script **version number** under a standard Google Cloud project, not a Google Workspace add-on deployment ID.

Align `src/appsscript.json`, the OAuth consent screen / data access configuration, and Google Workspace Marketplace SDK app configuration. The current feature set supports selecting multiple sheets, user-managed time triggers, and optional CSV export to a configured OSS bucket. No listing, approval or deployment is implied by local tests.

## Data access disclosure for owner review

- Spreadsheet scope: read/write query target sheets and reopen the recorded spreadsheet during scheduling. No Drive scope is requested.
- Trigger scope: install/remove a per-user scheduler trigger; users must explicitly enable scheduling.
- External requests: authenticated MaxCompute requests and optional OSS CSV uploads; OSS export sends sheet data to the configured bucket.
- User and script properties: credentials and preferences are stored by the existing Apps Script implementation. Operators must review sharing and script-property fallback behavior before distribution.
- `EXT_NODE_ONDUTY` records the Google account email that submitted the query; audit settings also identify the spreadsheet and target sheet.
- SQL history is stored locally and mirrored to user properties; users can disable future history and clear existing entries. Instance history expires after one day. Jobs and schedules have separate persistence and controls.

Support URL, privacy policy URL, terms URL, listing assets, retention policy, security contact, target visibility, OAuth verification status, and owner release approval: **TODO — external owner evidence required**.
