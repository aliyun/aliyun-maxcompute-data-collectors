// @ts-check

import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'MaxCompute Data Collectors',
  tagline: 'A collection of connectors and plugins for exchanging data with Alibaba Cloud MaxCompute',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://aliyun.github.io',
  baseUrl: '/aliyun-maxcompute-data-collectors/',

  organizationName: 'aliyun',
  projectName: 'aliyun-maxcompute-data-collectors',

  // Using 'warn' because /google-sheets/ is a static HTML file in /static/ that
  // Docusaurus's broken link checker doesn't recognize as a valid route
  onBrokenLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en', 'zh'],
    localeConfigs: {
      en: {
        label: 'English',
      },
      zh: {
        label: '中文',
      },
    },
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl:
            'https://github.com/aliyun/aliyun-maxcompute-data-collectors/tree/google-sheets/docs/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      colorMode: {
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'MaxCompute Data Collectors',
        logo: {
          alt: 'MaxCompute Data Collectors',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'connectorsSidebar',
            position: 'left',
            label: 'Connectors',
          },
          {
            href: 'https://aliyun.github.io/aliyun-maxcompute-data-collectors/google-sheets/',
            label: 'Google Sheets',
            position: 'left',
          },
          {
            type: 'localeDropdown',
            position: 'right',
          },
          {
            href: 'https://github.com/aliyun/aliyun-maxcompute-data-collectors',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Connectors',
            items: [
              {
                label: 'Spark Connector',
                to: '/docs/spark-connector/overview',
              },
              {
                label: 'Flink Connector',
                to: '/docs/flink-connector/overview',
              },
              {
                label: 'Flink CDC',
                href: 'https://github.com/apache/flink-cdc',
              },
              {
                label: 'Kafka Connector',
                href: 'https://github.com/aliyun/maxcompute-kafka-connector',
              },
            ],
          },
          {
            title: 'More Connectors',
            items: [
              {
                label: 'Trino Connector',
                to: '/docs/trino-connector/overview',
              },
              {
                label: 'StarRocks Connector',
                href: 'https://www.alibabacloud.com/help/en/maxcompute/user-guide/starrocks-connector',
              },
              {
                label: 'dbt-maxcompute',
                href: 'https://github.com/aliyun/dbt-maxcompute',
              },
              {
                label: 'Google Sheets',
                href: 'https://aliyun.github.io/aliyun-maxcompute-data-collectors/google-sheets/',
              },
            ],
          },
          {
            title: 'Resources',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/aliyun/aliyun-maxcompute-data-collectors',
              },
              {
                label: 'Alibaba Cloud MaxCompute',
                href: 'https://www.alibabacloud.com/product/maxcompute',
              },
            ],
          },
          {
            title: 'Legal',
            items: [
              {
                label: 'Privacy Policy',
                href: 'https://www.alibabacloud.com/help/en/legal/latest/alibaba-cloud-international-website-privacy-policy',
              },
              {
                label: 'Terms of Service',
                href: 'https://www.alibabacloud.com/help/en/legal/latest/international-website-product-terms-of-service',
              },
            ],
          },
        ],
        copyright: `Copyright \u00A9 ${new Date().getFullYear()} Alibaba Cloud. All rights reserved. Built with Docusaurus.<br/>All product names, logos, and trademarks are the property of their respective owners and are used here for identification purposes only.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['java', 'scala', 'bash', 'sql', 'properties'],
      },
    }),
};

export default config;
