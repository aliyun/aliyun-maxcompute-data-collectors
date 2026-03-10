# MaxCompute Data Collectors - Documentation Site

This directory contains the [Docusaurus](https://docusaurus.io/) project that powers the documentation website for MaxCompute Data Collectors.

**Live site**: https://aliyun.github.io/aliyun-maxcompute-data-collectors/

## Site Structure

| Path | Content | Source |
|------|---------|--------|
| `/` | Home page with connector catalog | `src/pages/index.js` |
| `/google-sheets/` | Google Sheets connector landing page | `static/google-sheets/index.html` |
| `/google-sheets/privacy.html` | Google Sheets privacy policy | `static/google-sheets/privacy.html` |
| `/docs/<connector>/` | Connector documentation (Markdown) | `docs/<connector>/` |

## Local Development

### Prerequisites

- Node.js >= 20
- pnpm (`npm install -g pnpm`)

### Setup

```bash
cd docs
pnpm install
```

### Start Development Server

```bash
pnpm start
```

This starts a local dev server at http://localhost:3000/aliyun-maxcompute-data-collectors/ with hot reload.

### Build

```bash
pnpm build
```

The static site is generated into the `build/` directory.

### Preview Build

```bash
pnpm serve
```

### i18n (Chinese)

To start the dev server in Chinese:

```bash
pnpm start -- --locale zh
```

To build the Chinese version:

```bash
pnpm build -- --locale zh
```

## Adding a New Connector

1. Create a new directory under `docs/docs/<connector-name>/`
2. Add an `overview.md` with frontmatter:
   ```markdown
   ---
   sidebar_position: 1
   title: Overview
   ---

   # Connector Name

   Description here...
   ```
3. The sidebar is auto-generated from the directory structure
4. Add a card entry in `src/pages/index.js` in the `connectors` array
5. Add Chinese translations in `i18n/zh/` if needed

## Deployment

The site is deployed automatically via GitHub Actions on push to the `google-sheets` branch. See `.github/workflows/deploy-docs.yml`.

### GitHub Pages Setup

In the repository Settings > Pages:
- **Source**: GitHub Actions (not "Deploy from a branch")

## Technology

- **Docusaurus 3.x** - Static site generator with React
- **i18n** - Built-in internationalization (English + Chinese)
- **Markdown/MDX** - For connector documentation
- **Static HTML** - Google Sheets landing page (preserved as-is)
