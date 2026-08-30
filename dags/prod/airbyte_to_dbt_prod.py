"""
prod/airbyte_to_dbt_prod.py

Prod pipeline: self-hosted Airbyte on EC2 reading RDS PostgreSQL (CDC),
landing in S3, then dbt build into the prod Snowflake GOLD schema.
Runs on MWAA. Config comes from AWS Secrets Manager since MWAA has
native IAM access to it and secrets shouldn't live in Variables here.

Secrets expected:
  olist/prod/airbyte-config        -> {host, workspace_id, client_id, client_secret}
  olist/prod/github-dbt-deploy-key -> {private_key, repo_url}
  olist/prod/snowflake-credentials -> {account, user, password, role, warehouse, database}
"""

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.append("/usr/local/airflow/dags")  # MWAA's dags/ folder
from common.airbyte_dbt_common import (
    DEFAULT_ARGS,
    get_secret,
    make_fetch_repo,
    make_list_connections,
    make_trigger_syncs,
    make_wait_for_syncs,
    make_write_profiles,
)

AIRBYTE_SECRET = "olist/prod/airbyte-config"
GITHUB_SECRET = "olist/prod/github-dbt-deploy-key"
SNOWFLAKE_SECRET = "olist/prod/snowflake-credentials"
DBT_PROJECT_DIR = "/usr/local/airflow/dbt_project"
DBT_PROFILES_DIR = "/tmp/.dbt"
DBT_TARGET = "prod"
DBT_SCHEMA = "GOLD"
DBT_DOCS_BUCKET = os.environ.get("DBT_DOCS_BUCKET", "olist-mwaa-data-724769809986")


def get_airbyte_cfg():
    return get_secret(AIRBYTE_SECRET)


def get_github_cfg():
    return get_secret(GITHUB_SECRET)


def get_snowflake_cfg():
    return get_secret(SNOWFLAKE_SECRET)


with DAG(
    dag_id="airbyte_to_dbt_pipeline_prod",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",  # hourly
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["airbyte", "dbt", "snowflake", "olist", "prod", "rds", "s3"],
) as dag:

    t_list = PythonOperator(
        task_id="list_connections",
        python_callable=make_list_connections(get_airbyte_cfg),
    )

    t_trigger = PythonOperator(
        task_id="trigger_syncs",
        python_callable=make_trigger_syncs(get_airbyte_cfg),
    )

    t_wait = PythonOperator(
        task_id="wait_for_syncs",
        python_callable=make_wait_for_syncs(get_airbyte_cfg),
    )

    t_pull_repo = PythonOperator(
        task_id="pull_dbt_repo",
        python_callable=make_fetch_repo(get_github_cfg, DBT_PROJECT_DIR),
    )

    t_write_profiles = PythonOperator(
        task_id="write_dbt_profiles",
        python_callable=make_write_profiles(get_snowflake_cfg, DBT_TARGET, DBT_SCHEMA, DBT_PROFILES_DIR),
    )

    t_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR}/olist_ecommerce && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    t_stage_ext = BashOperator(
        task_id="stage_external_sources",
        bash_command=(
            f"cd {DBT_PROJECT_DIR}/olist_ecommerce && "
            f"dbt run-operation stage_external_sources --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}"
        ),
    )

    t_dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR}/olist_ecommerce && dbt build --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}",
    )

    t_dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR}/olist_ecommerce && dbt docs generate --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET} "
            f"&& aws s3 sync target/ s3://{DBT_DOCS_BUCKET}/dbt-docs/ --delete"
        ),
    )

    t_list >> t_trigger >> t_wait
    t_wait >> t_pull_repo >> t_write_profiles
    t_write_profiles >> t_dbt_deps >> t_stage_ext >> t_dbt_build >> t_dbt_docs
