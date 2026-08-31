FROM apache/airflow:2.10.3-python3.11

USER root

# git + openssh-client: needed for pull_dbt_repo's `git clone`/`git fetch`
# over SSH using the GitHub deploy key
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    openssh-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user before installing Python packages
USER airflow

# Python deps used across your DAGs:
# - dbt-snowflake: dbt build/deps/docs against Snowflake
# - requests: Airbyte API calls
# - boto3: AWS Secrets Manager (prod DAG only, but harmless to include for dev too)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt