# MaxCompute Connector for Google Sheets - GitHub Pages

This directory contains the GitHub Pages website for the MaxCompute Connector for Google Sheets add-on.

## Setup Instructions

### 1. Enable GitHub Pages

1. Go to your GitHub repository settings
2. Navigate to **Pages** section
3. Under **Source**, select **Deploy from a branch**
4. Select the branch (e.g., `master` or `main`) and choose `/docs` folder
5. Click **Save**
6. Your site will be available at: `https://<your-org>.github.io/<repo-name>/`

### 2. Replace Placeholders

Before publishing, update the following placeholders in `index.html`:

#### Google Workspace Marketplace Installation URL
Find the install button and replace `#` with your actual Marketplace URL:
```html
<!-- Before -->
<a href="#" class="install-btn">

<!-- After -->
<a href="https://workspace.google.com/marketplace/app/your-app-id" class="install-btn">
```

#### OG URL (Optional)
Update the Open Graph URL for better social sharing:
```html
<!-- Uncomment and update -->
<meta property="og:url" content="https://<your-org>.github.io/<repo-name>/">
```

### 3. Google Search Console Verification

To verify domain ownership for Google OAuth consent screen:

1. Go to [Google Search Console](https://search.google.com/search-console)
2. Add a new property with your GitHub Pages URL
3. Choose **URL prefix** method
4. Download the HTML verification file (e.g., `google1234567890abcdef.html`)
5. Place the file in this `docs/` directory
6. Commit and push the changes
7. Click **Verify** in Google Search Console

### 4. Update OAuth Consent Screen

After your GitHub Pages is live, update your Google Cloud Console OAuth consent screen:

| Field | Value |
|-------|-------|
| Application name | `MaxCompute Connector for Google Sheets` |
| Application home page | `https://<your-org>.github.io/<repo-name>/` |
| Application privacy policy link | `https://www.alibabacloud.com/help/en/legal/latest/alibaba-cloud-international-website-privacy-policy` |
| Application terms of service link | `https://www.alibabacloud.com/help/en/legal/latest/international-website-product-terms-of-service` |

## File Structure

```
docs/
├── index.html          # Main landing page (bilingual: EN/ZH)
├── privacy.html        # Redirect to Alibaba Cloud Privacy Policy
├── README.md           # This file
└── google*.html        # Google Search Console verification files (add as needed)
```

## Features

- **Bilingual Support**: English and Chinese, with automatic language preference saving
- **Responsive Design**: Works on desktop and mobile devices
- **No External Dependencies**: Pure HTML, CSS, and vanilla JavaScript
- **OAuth Compliant**: Includes required privacy policy and terms of service links

## Local Testing

To preview the site locally, you can use Python's built-in HTTP server:

```bash
cd docs
python3 -m http.server 8000
```

Then open http://localhost:8000 in your browser.
