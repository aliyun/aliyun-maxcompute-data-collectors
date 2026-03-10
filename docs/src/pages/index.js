import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import Translate, {translate} from '@docusaurus/Translate';
import useBaseUrl from '@docusaurus/useBaseUrl';

import styles from './index.module.css';

const GITHUB_BASE = 'https://github.com/aliyun/aliyun-maxcompute-data-collectors/tree/master';

// Ordered by upstream open-source community influence
const activeConnectors = [
  {
    logo: 'img/connectors/spark.svg',
    title: 'Spark Connector',
    description: 'Read and write MaxCompute tables using Apache Spark.',
    link: '/docs/spark-connector/overview',
  },
  {
    logo: 'img/connectors/flink.svg',
    title: 'Flink Connector',
    description: 'Stream data between Apache Flink and MaxCompute.',
    link: '/docs/flink-connector/overview',
  },
  {
    logo: 'img/connectors/flink-cdc.svg',
    title: 'Flink CDC',
    description: 'Capture database changes in real-time and sync to MaxCompute via Flink CDC.',
    link: 'https://github.com/apache/flink-cdc',
    isExternal: true,
  },
  {
    logo: 'img/connectors/kafka.png',
    title: 'Kafka Connector',
    description: 'Stream data from Apache Kafka topics directly into MaxCompute.',
    link: 'https://github.com/aliyun/maxcompute-kafka-connector',
    isExternal: true,
  },
  {
    logo: 'img/connectors/trino.svg',
    title: 'Trino Connector',
    description: 'Query MaxCompute data using Trino (formerly PrestoSQL).',
    link: '/docs/trino-connector/overview',
  },
  {
    logo: 'img/connectors/starrocks.svg',
    title: 'StarRocks Connector',
    description: 'Exchange data between StarRocks and MaxCompute for high-performance analytics.',
    link: 'https://www.alibabacloud.com/help/en/maxcompute/user-guide/starrocks-connector',
    isExternal: true,
  },
  {
    logo: 'img/connectors/dbt.svg',
    title: 'dbt-maxcompute',
    description: 'Build and manage data transformation pipelines on MaxCompute using dbt.',
    link: 'https://github.com/aliyun/dbt-maxcompute',
    isExternal: true,
  },
  {
    logo: 'img/connectors/grafana.svg',
    title: 'Grafana Plugin',
    description: 'Visualize MaxCompute data in Grafana dashboards.',
    badge: 'coming-soon',
  },
  {
    logo: 'img/connectors/presto.svg',
    title: 'Presto Connector',
    description: 'Query MaxCompute data using PrestoDB.',
    link: '/docs/presto-connector/overview',
  },
  {
    logo: 'img/connectors/metabase.svg',
    title: 'Metabase Driver',
    description: 'Connect Metabase to MaxCompute for BI and analytics.',
    link: '/docs/metabase-driver/overview',
  },
  {
    logo: 'img/connectors/google-sheets.svg',
    title: 'Google Sheets Connector',
    description: 'Query Alibaba Cloud MaxCompute data directly in Google Sheets.',
    link: 'https://aliyun.github.io/aliyun-maxcompute-data-collectors/google-sheets/',
    isExternal: true,
  },
];

const archivedConnectors = [
  {
    icon: '\u{1F4BE}',
    title: 'OGG Plugin',
    description: 'Real-time data replication from Oracle to MaxCompute via GoldenGate.',
    link: `${GITHUB_BASE}/ogg-plugin`,
    isExternal: true,
  },
  {
    icon: '\u{1F527}',
    title: 'Kettle Plugin',
    description: 'ETL integration with Pentaho Data Integration (Kettle).',
    link: `${GITHUB_BASE}/kettle-plugin`,
    isExternal: true,
  },
  {
    icon: '\u{1F504}',
    title: 'Sqoop',
    description: 'Bulk transfer data between MaxCompute and relational databases.',
    link: `${GITHUB_BASE}/odps-sqoop`,
    isExternal: true,
  },
  {
    icon: '\u{1F4E8}',
    title: 'Flume Plugin',
    description: 'Collect and ingest log data into MaxCompute using Apache Flume.',
    link: `${GITHUB_BASE}/flume-plugin`,
    isExternal: true,
  },
  {
    icon: '\u{1F680}',
    title: 'Spark DataSource V2',
    description: 'Spark DataSource V2 API for MaxCompute (Spark 2.3 & 3.1).',
    link: `${GITHUB_BASE}/spark-datasource-v3.1`,
    isExternal: true,
  },
  {
    icon: '\u{1F41D}',
    title: 'Hive Data Transfer',
    description: 'Transfer data between Hive and MaxCompute using UDTFs.',
    link: `${GITHUB_BASE}/hive_data_transfer_udtf`,
    isExternal: true,
  },
];

function ConnectorCard({logo, icon, title, description, link, isExternal, badge, archived}) {
  const hasLink = !!link;
  const CardTag = !hasLink ? 'div' : isExternal ? 'a' : Link;
  const cardProps = !hasLink
    ? {}
    : isExternal
      ? {href: link, target: '_blank', rel: 'noopener noreferrer'}
      : {to: link};
  const logoUrl = logo ? useBaseUrl(logo) : null;

  return (
    <CardTag
      className={clsx(
        'connector-card',
        archived && 'connector-card--archived',
        !hasLink && 'connector-card--no-link',
      )}
      {...cardProps}
    >
      <div className="connector-card__icon">
        {logoUrl ? (
          <img src={logoUrl} alt={title} className="connector-card__logo" />
        ) : (
          icon
        )}
      </div>
      <div className="connector-card__title">{title}</div>
      <div className="connector-card__description">{description}</div>
      {badge === 'coming-soon' && (
        <span className="connector-card__badge connector-card__badge--coming-soon">
          <Translate id="homepage.badge.comingSoon">Open Source Soon</Translate>
        </span>
      )}
      {archived && (
        <span className="connector-card__badge connector-card__badge--archived">
          <Translate id="homepage.badge.archived">Archived</Translate>
        </span>
      )}
    </CardTag>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--mc', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title" style={{color: 'white'}}>
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle" style={{color: 'rgba(255,255,255,0.9)'}}>
          <Translate id="homepage.tagline">
            A collection of connectors and plugins for exchanging data with Alibaba Cloud MaxCompute
          </Translate>
        </p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/spark-connector/overview">
            <Translate id="homepage.cta">Get Started</Translate>
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={translate({id: 'homepage.title', message: 'Home'})}
      description={siteConfig.tagline}>
      <HomepageHeader />
      <main>
        {/* Active Connectors */}
        <section className="container" style={{paddingTop: '2rem', paddingBottom: '2rem'}}>
          <Heading as="h2" style={{textAlign: 'center', marginBottom: '0.5rem'}}>
            <Translate id="homepage.connectors.title">Connectors & Plugins</Translate>
          </Heading>
          <p style={{textAlign: 'center', color: 'var(--ifm-color-emphasis-700)', marginBottom: '2rem'}}>
            <Translate id="homepage.connectors.subtitle">
              Choose a connector to integrate with MaxCompute
            </Translate>
          </p>
          <div className="connector-grid">
            {activeConnectors.map((connector, idx) => (
              <ConnectorCard key={idx} {...connector} />
            ))}
          </div>
        </section>

        {/* Archived Connectors */}
        <section className="container" style={{paddingTop: '1rem', paddingBottom: '3rem'}}>
          <Heading as="h2" style={{textAlign: 'center', marginBottom: '0.5rem', fontSize: '1.5rem'}}>
            <Translate id="homepage.archived.title">Archived</Translate>
          </Heading>
          <p style={{textAlign: 'center', color: 'var(--ifm-color-emphasis-600)', marginBottom: '1.5rem', fontSize: '0.9rem'}}>
            <Translate id="homepage.archived.subtitle">
              These connectors are no longer actively maintained. Source code is available on GitHub.
            </Translate>
          </p>
          <div className="connector-grid">
            {archivedConnectors.map((connector, idx) => (
              <ConnectorCard key={idx} {...connector} archived />
            ))}
          </div>
        </section>
      </main>
    </Layout>
  );
}
