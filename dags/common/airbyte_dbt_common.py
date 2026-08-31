from datetime import timedelta
import json
import os
import subprocess
import time

import requests

POLL_INTERVAL_SEC = 30
SYNC_TIMEOUT_SEC = 60 * 60


# ---------------------------------------------------------------------------
# AWS Secrets Manager helper — only used by prod's DAG, not imported by dev
# ---------------------------------------------------------------------------
def get_secret(secret_id: str, region: str = None) -> dict:
    import boto3
    region = region or os.environ.get("AWS_REGION", "us-east-1")
    client = boto3.client("secretsmanager", region_name=region)
    return json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])


# ---------------------------------------------------------------------------
# Airbyte auth + base URL — everything here takes a plain dict:
# ---------------------------------------------------------------------------
def get_airbyte_token(cfg: dict) -> str:
    resp = requests.post(
        f"{cfg['host']}/api/public/v1/applications/token",
        json={
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def airbyte_headers(cfg: dict) -> dict:
    token = get_airbyte_token(cfg)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def airbyte_api_url(cfg: dict) -> str:
    return f"{cfg['host']}/api/v1"


# ---------------------------------------------------------------------------
# Airbyte callables — take a no-arg function that returns the config dict,
# so the dict is only built at task-run time (not at DAG-parse time)
# ---------------------------------------------------------------------------
def make_list_connections(get_cfg):
    def _list_connections(**context):
        cfg = get_cfg()
        base_url = airbyte_api_url(cfg)
        headers = airbyte_headers(cfg)

        resp = requests.post(
            f"{base_url}/connections/list",
            json={"workspaceId": cfg["workspace_id"]},
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()

        connections = resp.json().get("connections", [])
        if not connections:
            raise ValueError(
                "No Airbyte connections found in workspace — check workspace_id in config."
            )

        connection_ids = [c["connectionId"] for c in connections]
        print(f"Found {len(connection_ids)} connection(s): {connection_ids}")
        context["ti"].xcom_push(key="connection_ids", value=connection_ids)

    return _list_connections


def make_trigger_syncs(get_cfg):
    def _trigger_syncs(**context):
        cfg = get_cfg()
        base_url = airbyte_api_url(cfg)
        headers = airbyte_headers(cfg)

        connection_ids = context["ti"].xcom_pull(
            task_ids="list_connections", key="connection_ids"
        )

        job_ids = []
        for conn_id in connection_ids:
            resp = requests.post(
                f"{base_url}/connections/sync",
                json={"connectionId": conn_id},
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            job_id = resp.json()["job"]["id"]
            print(f"Triggered sync for connection {conn_id} -> job {job_id}")
            job_ids.append(job_id)

        context["ti"].xcom_push(key="job_ids", value=job_ids)

    return _trigger_syncs


def make_wait_for_syncs(get_cfg):
    def _wait_for_syncs(**context):
        cfg = get_cfg()
        base_url = airbyte_api_url(cfg)

        job_ids = context["ti"].xcom_pull(task_ids="trigger_syncs", key="job_ids")
        pending = set(job_ids)
        start = time.time()

        while pending:
            if time.time() - start > SYNC_TIMEOUT_SEC:
                raise TimeoutError(
                    f"Airbyte jobs {pending} did not finish within {SYNC_TIMEOUT_SEC}s"
                )

            headers = airbyte_headers(cfg)  # refresh token each loop — tokens are short-lived

            for job_id in list(pending):
                resp = requests.post(
                    f"{base_url}/jobs/get",
                    json={"id": job_id},
                    headers=headers,
                    timeout=30,
                )
                resp.raise_for_status()

                status = resp.json()["job"]["status"]
                print(f"Job {job_id}: {status}")

                if status == "succeeded":
                    pending.discard(job_id)
                elif status in ("failed", "cancelled", "incomplete"):
                    raise RuntimeError(
                        f"Airbyte job {job_id} ended with status '{status}'. Check UI logs."
                    )

            if pending:
                print(f"Waiting for jobs: {pending} — sleeping {POLL_INTERVAL_SEC}s")
                time.sleep(POLL_INTERVAL_SEC)

        print("All Airbyte sync jobs completed successfully.")

    return _wait_for_syncs


# ---------------------------------------------------------------------------
# dbt callables — same pattern, take a get_cfg() function for GitHub /
# Snowflake config
# ---------------------------------------------------------------------------
def make_fetch_repo(get_github_cfg, dbt_project_dir: str):
    def _fetch_repo(**context):
        gh = get_github_cfg()

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

        if os.path.isdir(os.path.join(dbt_project_dir, ".git")):
            subprocess.run(
                f"cd {dbt_project_dir} && git fetch origin main && git reset --hard origin/main",
                shell=True, check=True,
            )
        else:
            os.makedirs(dbt_project_dir, exist_ok=True)
            subprocess.run(
                f"git clone --branch main --depth 1 {gh['repo_url']} {dbt_project_dir}",
                shell=True, check=True,
            )

    return _fetch_repo


def make_write_profiles(get_snowflake_cfg, dbt_target: str, dbt_schema: str, profiles_dir: str):
    def _write_profiles(**context):
        sf = get_snowflake_cfg()
        os.makedirs(profiles_dir, exist_ok=True)

        profiles_yml = f"""
olist_ecommerce:
  target: {dbt_target}
  outputs:
    {dbt_target}:
      type: snowflake
      account: {sf['account']}
      user: {sf['user']}
      password: {sf['password']}
      role: {sf['role']}
      warehouse: {sf['warehouse']}
      database: {sf['database']}
      schema: {dbt_schema}
      threads: 4
"""
        with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
            f.write(profiles_yml)

    return _write_profiles


DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
