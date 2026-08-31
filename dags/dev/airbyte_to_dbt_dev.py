import sys
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/dags")  
from common.airbyte_dbt_common import (
    DEFAULT_ARGS,
    make_fetch_repo,
    make_list_connections,
    make_trigger_syncs,
    make_wait_for_syncs,
    make_write_profiles,
)

DBT_PROJECT_DIR = "/tmp/dbt_project_dev"
DBT_PROFILES_DIR = "/tmp/.dbt_dev"
DBT_TARGET = "dev"
DBT_SCHEMA = "DEV"


def get_airbyte_cfg():
    return {
        "host": Variable.get("airbyte_dev_host", default_var="http://172.21.0.2:80"),
        "workspace_id": Variable.get("workspace_id"),
        "client_id": Variable.get("client_id"),
        "client_secret": Variable.get("client_secret"),
    }


def get_github_cfg():
    return {
        "repo_url": Variable.get("repo_url"),
        "private_key": Variable.get("private_key"),
    }


def get_snowflake_cfg():
    return {
        "account": Variable.get("account"),
        "user": Variable.get("user"),
        "password": Variable.get("password"),
        "role": Variable.get("role"),
        "warehouse": Variable.get("warehouse"),
        "database": Variable.get("database"),
    }


with DAG(
    dag_id="airbyte_to_dbt_pipeline_dev",
    default_args=DEFAULT_ARGS,
    schedule_interval=None, 
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["airbyte", "dbt", "snowflake", "olist", "dev"],
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

    t_list >> t_trigger >> t_wait
    t_wait >> t_pull_repo >> t_write_profiles
    t_write_profiles >> t_dbt_deps >> t_stage_ext >> t_dbt_build
