"""
dbt_pipeline_dag.py

Pulls the latest `main` of the dbt project via a read-only GitHub deploy
key, then runs `dbt build` against Snowflake. Designed to run AFTER the
Airbyte sync DAG completes, so SILVER/GOLD always builds from fresh BRONZE
data.

Credentials (Snowflake + GitHub deploy key) are pulled from Secrets Manager
at task runtime — nothing is baked into the DAG file or the MWAA image.
"""
from datetime import datetime, timedelta
import json
import os

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

PROJECT = "olist"
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
DBT_PROJECT_DIR = "/usr/local/airflow/dbt_project"  # ephemeral worker-local path


def _get_secret(secret_id: str) -> dict:
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    return json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])


def fetch_repo(**context):
    """Clone (or pull) the dbt project using a read-only deploy key."""
    gh = _get_secret(f"{PROJECT}/github-dbt-deploy-key")

    ssh_dir = "/tmp/.ssh"
    os.makedirs(ssh_dir, exist_ok=True, mode=0o700)
    key_path = os.path.join(ssh_dir, "deploy_key")
    with open(key_path, "w") as f:
        f.write(gh["private_key"])
    os.chmod(key_path, 0o600)

    git_ssh_cmd = (
        f"ssh -i {key_path} -o StrictHostKeyChecking=accept-new "
        f"-o UserKnownHostsFile=/tmp/.ssh/known_hosts"
    )
    os.environ["GIT_SSH_COMMAND"] = git_ssh_cmd

    if os.path.isdir(os.path.join(DBT_PROJECT_DIR, ".git")):
        os.system(f"cd {DBT_PROJECT_DIR} && git fetch origin main && git reset --hard origin/main")
    else:
        os.makedirs(DBT_PROJECT_DIR, exist_ok=True)
        os.system(f"git clone --branch main --depth 1 {gh['repo_url']} {DBT_PROJECT_DIR}")


def write_profiles(**context):
    """Write profiles.yml from Snowflake creds in Secrets Manager."""
    sf = _get_secret(f"{PROJECT}/snowflake-credentials")
    profiles_dir = "/tmp/.dbt"
    os.makedirs(profiles_dir, exist_ok=True)

    profiles_yml = f"""
olist_ecommerce:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: {sf['account']}
      user: {sf['user']}
      password: {sf['password']}
      role: {sf['role']}
      warehouse: {sf['warehouse']}
      database: {sf['database']}
      schema: GOLD
      threads: 4
"""
    with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
        f.write(profiles_yml)


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_build_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "snowflake", "olist"],
) as dag:

    # Wait for the upstream Airbyte sync DAG to finish so BRONZE is fresh
    # before SILVER/GOLD builds. Adjust external_dag_id to match your
    # actual Airbyte-trigger DAG name.
    wait_for_airbyte_sync = ExternalTaskSensor(
        task_id="wait_for_airbyte_sync",
        external_dag_id="airbyte_sync_pipeline",
        timeout=60 * 60,
        poke_interval=60,
        mode="reschedule",
    )

    pull_dbt_repo = PythonOperator(
        task_id="pull_dbt_repo",
        python_callable=fetch_repo,
    )

    write_dbt_profiles = PythonOperator(
        task_id="write_dbt_profiles",
        python_callable=write_profiles,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir /tmp/.dbt",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt build --profiles-dir /tmp/.dbt --target prod",
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir /tmp/.dbt --target prod "
            f"&& aws s3 sync target/ s3://{os.environ.get('DBT_DOCS_BUCKET', 'SET_DBT_DOCS_BUCKET_ENV_VAR')}/dbt-docs/ --delete"
        ),
    )

    wait_for_airbyte_sync >> pull_dbt_repo >> write_dbt_profiles >> dbt_deps >> dbt_build >> dbt_docs_generate
