"""
airbyte_sync_pipeline.py

Triggers all Airbyte connections (RDS → S3 staging) via the Airbyte API,
polls until every sync job completes, then succeeds so the downstream
dbt_build_pipeline DAG can start via ExternalTaskSensor.

Infrastructure notes (from terraform):
  - Airbyte EC2 is in a private subnet; MWAA reaches it on port 8006
    (Airbyte internal API) via VPC routing — no public IP needed.
  - Airbyte private IP is stored in Secrets Manager under
    olist/airbyte-config so we never hard-code it here.
  - airbyte_sg allows inbound 8000-8001 from MWAA CIDR; port 8006

"""

from datetime import datetime, timedelta
import json
import os
import time

import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT     = "olist"
AWS_REGION  = os.environ.get("AWS_REGION", "us-east-1")
POLL_INTERVAL_SEC = 30   # how often to poll Airbyte for job status
SYNC_TIMEOUT_SEC  = 60 * 60  # 1 hour max before we give up


def _get_secret(secret_id: str) -> dict:
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    return json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])


def _airbyte_base_url() -> str:
    """
    Reads Airbyte private IP from Secrets Manager.
    Store the IP after `terraform output airbyte_instance_private_ip`:
        aws secretsmanager put-secret-value \
            --secret-id olist/airbyte-config \
            --secret-string '{"private_ip": "<IP>", "workspace_id": "<UUID>"}'
    """
    cfg = _get_secret(f"{PROJECT}/airbyte-config")
    return f"http://{cfg['private_ip']}:8006/api/v1"


def _get_workspace_id() -> str:
    cfg = _get_secret(f"{PROJECT}/airbyte-config")
    return cfg["workspace_id"]


def list_connections(**context):
    """
    Fetches all connections in the Airbyte workspace and pushes their IDs
    to XCom so the next task knows what to trigger.
    """
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

    # Push to XCom so trigger_syncs can read them
    context["ti"].xcom_push(key="connection_ids", value=connection_ids)


def trigger_syncs(**context):
    """
    Triggers a sync job for every connection found by list_connections,
    then pushes the resulting job IDs to XCom for the polling task.
    """
    base_url       = _airbyte_base_url()
    connection_ids = context["ti"].xcom_pull(
        task_ids="list_connections", key="connection_ids"
    )

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
    """
    Polls every triggered job until all are 'succeeded'.
    Raises on any 'failed' / 'cancelled' status, or if timeout is hit.
    """
    base_url = _airbyte_base_url()
    job_ids  = context["ti"].xcom_pull(task_ids="trigger_syncs", key="job_ids")

    pending   = set(job_ids)
    start     = time.time()

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
                raise RuntimeError(
                    f"Airbyte job {job_id} ended with status '{status}'. "
                    "Check the Airbyte UI for logs."
                )
            # "running" / "pending" → keep waiting

        if pending:
            print(f"Still waiting for jobs: {pending} — sleeping {POLL_INTERVAL_SEC}s")
            time.sleep(POLL_INTERVAL_SEC)

    print("All Airbyte sync jobs completed successfully.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="airbyte_sync_pipeline",   
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["airbyte", "rds", "s3", "olist"],
) as dag:

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

    # Chain: discover → trigger → poll until done
    # dbt_build_pipeline's ExternalTaskSensor watches THIS dag finishing,
    # so dbt only starts once all S3 raw files are fresh.
    t_list >> t_trigger >> t_wait
