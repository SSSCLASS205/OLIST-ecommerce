import boto3
import os 
import json
from dotenv import load_dotenv

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
PROJECT_NAME = "olist"  

ENV_PATH = '/home/sssclass/Desktop/project/olist_project/dbt_project/.env'
load_dotenv(dotenv_path=ENV_PATH)


def update_aws_secret(secret_name, data_dict):

    client = boto3.client('secretsmanager', region_name=AWS_REGION)
    
    # Check if any values are missing before pushing
    if any(v is None for v in data_dict.values()):
        print(f"⚠ Skipping {secret_name}: One or more environment variables were not found in .env")
        return

    try:
        # Convert dictionary to JSON string
        json_payload = json.dumps(data_dict)
        
        # Send to AWS
        client.put_secret_value(
            SecretId=secret_name,
            SecretString=json_payload
        )
        print(f"Successfully updated AWS Secret: {secret_name}")
        
    except client.exceptions.ResourceNotFoundException:
        print(f"Error: The secret '{secret_name}' does not exist in AWS. Did you run 'terraform apply' first?")
    except Exception as e:
        print(f"Failed to update {secret_name}: {str(e)}")


def main():
    print("Starting automation script: Syncing .env to AWS Secrets Manager...")

    # --- 1. Structure Snowflake Payload ---
    snowflake_payload = {
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "role":      os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database":  os.getenv("SNOWFLAKE_DATABASE")
    }

    # --- 2. Structure GitHub Deploy Key Payload ---
    raw_private_key = os.getenv("GITHUB_PRIVATEKEY", "")
    # Safely convert escaped string \n back to true layout breaks if necessary
    formatted_private_key = raw_private_key.replace("\\n", "\n") if raw_private_key else ""

    github_payload = {
        "private_key": formatted_private_key,
        "repo_url":    os.getenv("GITHUB_REPOURL")
    }

    # --- 3. Structure Airbyte Payload ---
    airbyte_payload = {
        "private_ip":   os.getenv("AIRBYTE_PRIVATEIP"),
        "workspace_id": os.getenv("AIRBYTE_WORKSPACEID")
    }

    # --- 4. Execute Updates ---
    # Container names mirror your Terraform setup: "${var.project}/secret-name"
    update_aws_secret(f"{PROJECT_NAME}/snowflake-credentials", snowflake_payload)
    update_aws_secret(f"{PROJECT_NAME}/github-dbt-deploy-key", github_payload)
    update_aws_secret(f"{PROJECT_NAME}/airbyte-config", airbyte_payload)

    print("Sync process complete.")


if __name__ == "__main__":
    main()