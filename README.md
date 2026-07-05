# Olist E-Commerce Data Warehouse Pipeline

An end-to-end, CDC-based data pipeline that replicates a Brazilian e-commerce marketplace's OLTP data into a Snowflake warehouse — built entirely on AWS with Terraform, orchestrated by Managed Airflow (MWAA), and transformed with dbt into an analytics-ready star schema.

> Source data: the public [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset, seeded into a live Postgres instance to simulate a real production OLTP system.

---

## Why this project

Most "dbt + Snowflake" portfolio projects start from a static CSV. This one doesn't. It simulates a real operational database (RDS Postgres), captures changes the way a production system actually would (log-based CDC via Airbyte), and moves that data through a proper Bronze → Silver → Gold pipeline — with infrastructure as code, CI, alerting, and secrets management included.

## Architecture

```
┌─────────────┐   CDC (logical      ┌──────────────┐   Parquet     ┌─────────────┐
│  RDS         │   replication)      │   Airbyte     │   staging      │  S3          │
│  PostgreSQL  ├────────────────────►│   (EC2, ASG)  ├───────────────►│  (staging)   │
│  (OLTP)      │                     │               │                │              │
└─────────────┘                     └──────────────┘                └──────┬──────┘
                                                                            │ external tables
                                                                            ▼
┌─────────────┐   hourly DAG        ┌──────────────┐                ┌─────────────┐
│  MWAA        ├────────────────────►│  dbt build    ├───────────────►│  Snowflake   │
│  (Airflow)   │   trigger + poll    │  (Bronze→     │  BRONZE →      │  Warehouse   │
│              │   Airbyte, run dbt  │  Silver→Gold) │  SILVER →      │  (star       │
└─────────────┘                     └──────────────┘  GOLD          │  schema)     │
                                                                     └─────────────┘
```

The full VPC design (public/private subnets, NAT gateway, VPC endpoints for S3 and Secrets Manager, security groups) and every AWS resource are defined in `terraform/`.

## Tech stack

| Layer | Tool |
|---|---|
| Source OLTP | Amazon RDS for PostgreSQL 16 (logical replication enabled for CDC) |
| Change Data Capture | Airbyte, self-hosted on an EC2 Auto Scaling Group |
| Staging | Amazon S3 (Parquet) |
| Orchestration | Amazon MWAA (Managed Apache Airflow 2.9) |
| Transformation | dbt (dbt-snowflake) |
| Warehouse | Snowflake |
| Infrastructure | Terraform |
| Secrets | AWS Secrets Manager |
| Alerting | SNS + SQS (DLQ) + CloudWatch alarms |
| CI | GitHub Actions (dbt build on every PR touching models/tests) |

## How data flows

1. **Seed & simulate** — `scripts/olist_simulator.py` loads the historical slice of the Olist dataset into RDS, then replays the remaining rows as live `INSERT`/`UPDATE` traffic, so the pipeline has real change events to capture, not just a one-time dump.
2. **Capture changes** — RDS has logical replication turned on (`rds.logical_replication`, custom parameter group) so Airbyte can stream inserts/updates via CDC rather than repeatedly scanning full tables.
3. **Land raw data** — Airbyte syncs each table to S3 as Parquet, partitioned by source table.
4. **Expose as Bronze** — dbt's `dbt_external_tables` package registers those S3 files as Snowflake external tables (`models/BRONZE`).
5. **Clean & conform (Silver)** — dbt models standardize types, dedupe, and apply light business logic per source table (orders, order items, payments, reviews, products, geolocation).
6. **Track slow-changing attributes** — dbt snapshots (`check` strategy) maintain history for customers and sellers.
7. **Model the warehouse (Gold)** — a dimensional star schema: `fact_sales`, `fact_payment`, `fact_reviews` around `dim_customer`, `dim_product`, `dim_seller`, `dim_date`, `dim_geolocation`, `dim_order_attribute`, `dim_feedback_profile`. `fact_sales` is incrementally materialized on `_airbyte_emitted_at`.
8. **Orchestrate** — a single MWAA DAG (`dags/full_pipline.py`) runs hourly: trigger all Airbyte connections → poll until they succeed → pull the latest dbt project via a GitHub deploy key → `dbt build` against Snowflake → `dbt docs generate` and publish to S3.
9. **Validate** — custom dbt tests assert business invariants (e.g., delivery date can't precede purchase date, every fact_sales order actually exists, review answers can't predate review creation).
10. **Ship safely** — every PR touching models/snapshots/tests runs a GitHub Actions workflow that builds and tests the dbt project against an isolated Snowflake CI schema before merge.

## Repository structure

```
terraform/            All AWS infrastructure (VPC, RDS, S3, Airbyte EC2, MWAA, IAM, alerting)
dags/                 Airflow DAG: Airbyte trigger/poll + dbt build orchestration
olist_ecommerce/      dbt project
  models/BRONZE/      External table definitions over raw S3 data
  models/SILVER/      Cleaned, conformed staging models
  models/GOLD/        Star schema: facts + dimensions
  snapshots/          SCD-2 tracking for customers & sellers
  tests/              Custom data quality assertions
scripts/              CDC setup, data simulator, secrets publishing, DAG deployment
.github/workflows/    CI pipeline for dbt
```

## Data quality & reliability

- Custom dbt tests enforce referential and temporal integrity across the warehouse.
- SNS + SQS dead-letter queue + CloudWatch alarms on the MWAA scheduler catch pipeline failures.
- Snowflake credentials, RDS credentials, and the GitHub deploy key used by Airflow are all stored in AWS Secrets Manager — nothing is hardcoded.
- All data plane traffic between MWAA/Airbyte and AWS services stays inside the VPC via S3 and Secrets Manager VPC endpoints.

## Running it yourself

**Infrastructure**
```bash
cd terraform
terraform init
terraform apply
```

**Local dbt development** (against Airflow running locally via Docker)
```bash
docker compose up -d
uv sync   # or: pip install -r requirement.txt
cd olist_ecommerce
dbt deps
dbt build
```

## Possible next steps

- Replace polling-based Airbyte sync checks with event-driven webhooks
- Add dbt exposures / a BI layer on top of the Gold schema
- Parameterize environments (dev/prod) fully through Terraform workspaces
- Build a data quality metrics pipeline — track test pass rates, row-count deltas, freshness, and null/duplicate ratios over time (e.g. via `dbt artifacts` + a Gold-layer metrics table) instead of just pass/fail at build time
- Add system observability across the stack — centralized logging and dashboards for MWAA task duration/failures, Airbyte sync latency, and Snowflake warehouse credit usage, with alerts wired into the existing SNS topic

---

Built as a hands-on exploration of production-style CDC pipelines — feedback and PRs welcome.
