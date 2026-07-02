"""
airbyte_to_dbt_pipeline.py

Triggers all Airbyte connections (RDS → S3 staging) via the Airbyte API,
polls until every sync job completes, then pulls the latest dbt project 
via GitHub deploy key and runs dbt build against Snowflake.
"""

from datetime import datetime, timedelta
import json
import os
import time

import boto3
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Global Configurations
# ---------------------------------------------------------------------------
PROJECT           = "olist"
AWS_REGION        = os.environ.get("AWS_REGION", "us-east-1")
POLL_INTERVAL_SEC = 30         # How often to poll Airbyte for job status
SYNC_TIMEOUT_SEC  = 60 * 60    # 1 hour max before giving up on Airbyte syncs
DBT_PROJECT_DIR   = "/usr/local/airflow/dbt_project"


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------
def _get_secret(secret_id: str) -> dict:
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    return json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])

def _airbyte_base_url() -> str:
    cfg = _get_secret(f"{PROJECT}/airbyte-config")
    return f"http://{cfg['private_ip']}:8006/api/v1"

def _get_workspace_id() -> str:
    cfg = _get_secret(f"{PROJECT}/airbyte-config")
    return cfg["workspace_id"]


# ---------------------------------------------------------------------------
# Airbyte Python Callables
# ---------------------------------------------------------------------------
def list_connections(**context):
    base_url     = _airbyte_base_url()
    workspace_id = _get_workspace_id()

    resp = requests.post(
        f"{base_url}/connections/list",
        json={"workspaceId": workspace_id},
        timeout=30,
    )
    resp.raise_for_status()

    connections = resp.json().get("connections", [])
    if not connections:
        raise ValueError("No Airbyte connections found in workspace — check workspace_id in secret.")

    connection_ids = [c["connectionId"] for c in connections]
    print(f"Found {len(connection_ids)} connection(s): {connection_ids}")

    context["ti"].xcom_push(key="connection_ids", value=connection_ids)

def trigger_syncs(**context):
    base_url       = _airbyte_base_url()
    connection_ids = context["ti"].xcom_pull(task_ids="list_connections", key="connection_ids")

    job_ids = []
    for conn_id in connection_ids:
        resp = requests.post(
            f"{base_url}/connections/sync",
            json={"connectionId": conn_id},
            timeout=30,
        )
        resp.raise_for_status()
        job_id = resp.json()["job"]["id"]
        print(f"Triggered sync for connection {conn_id} → job {job_id}")
        job_ids.append(job_id)

    context["ti"].xcom_push(key="job_ids", value=job_ids)

def wait_for_syncs(**context):
    base_url = _airbyte_base_url()
    job_ids  = context["ti"].xcom_pull(task_ids="trigger_syncs", key="job_ids")

    pending = set(job_ids)
    start   = time.time()

    while pending:
        if time.time() - start > SYNC_TIMEOUT_SEC:
            raise TimeoutError(f"Airbyte jobs {pending} did not finish within {SYNC_TIMEOUT_SEC}s")

        for job_id in list(pending):
            resp = requests.post(
                f"{base_url}/jobs/get",
                json={"id": job_id},
                timeout=30,
            )
            resp.raise_for_status()

            status = resp.json()["job"]["status"]
            print(f"Job {job_id}: {status}")

            if status == "succeeded":
                pending.discard(job_id)
            elif status in ("failed", "cancelled", "incomplete"):
                raise RuntimeError(f"Airbyte job {job_id} ended with status '{status}'. Check UI logs.")

        if pending:
            print(f"Waiting for jobs: {pending} — sleeping {POLL_INTERVAL_SEC}s")
            time.sleep(POLL_INTERVAL_SEC)

    print("All Airbyte sync jobs completed successfully.")


# ---------------------------------------------------------------------------
# dbt Python Callables
# ---------------------------------------------------------------------------
def fetch_repo(**context):
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


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="airbyte_to_dbt_pipeline",
    default_args=default_args,
    schedule_interval="0 * * * *",  # Set to run hourly
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["airbyte", "dbt", "snowflake", "olist", "rds", "s3"],
) as dag:

    # 1. Airbyte Tasks
    t_list = PythonOperator(
        task_id="list_connections",
        python_callable=list_connections,
    )

    t_trigger = PythonOperator(
        task_id="trigger_syncs",
        python_callable=trigger_syncs,
    )

    t_wait = PythonOperator(
        task_id="wait_for_syncs",
        python_callable=wait_for_syncs,
    )

    # 2. dbt Setup Tasks
    t_pull_repo = PythonOperator(
        task_id="pull_dbt_repo",
        python_callable=fetch_repo,
    )

    t_write_profiles = PythonOperator(
        task_id="write_dbt_profiles",
        python_callable=write_profiles,
    )

    # 3. dbt Execution Tasks
    t_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR}/olist_ecommerce && dbt deps --profiles-dir /tmp/.dbt",
    )
    
    t_stage_ext = BashOperator(
        task_id="Stage_External_Sources",
        bash_command=f"cd {DBT_PROJECT_DIR}/olist_ecommerce && dbt run-operation stage_external_sources --profiles-dir /tmp/.dbt --target prod",
    )

    t_dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR}/olist_ecommerce && dbt build --profiles-dir /tmp/.dbt --target prod",
    )

    t_dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR}/olist_ecommerce && dbt docs generate --profiles-dir /tmp/.dbt --target prod "
            f"&& aws s3 sync target/ s3://{os.environ.get('DBT_DOCS_BUCKET', 'olist-mwaa-data-724769809986')}/dbt-docs/ --delete"
        ),
    )

    # -----------------------------------------------------------------------
    # Task Orchestration
    # -----------------------------------------------------------------------
    # Airbyte flow
    t_list >> t_trigger >> t_wait
    
    # Once Airbyte is done, set up dbt environment
    t_wait >> t_pull_repo >> t_write_profiles
    
    # Execute dbt commands sequentially
    t_write_profiles >> t_dbt_deps >> t_stage_ext >> t_dbt_build >> t_dbt_docs