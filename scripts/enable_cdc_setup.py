import os
import psycopg2
from psycopg2 import sql
from dotenv import find_dotenv, load_dotenv

# Load the environment variables from the .env file
load_dotenv(find_dotenv())

# ==========================================
# 1. CONFIGURATION (Loaded from .env)
# ==========================================
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

AIRBYTE_USER = os.getenv("AIRBYTE_USER")
AIRBYTE_PASS = os.getenv("AIRBYTE_PASS")
PUBLICATION_NAME = os.getenv("PUBLICATION_NAME")

def setup_database_for_cdc():
    # Quick check to ensure the env variables loaded
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS, AIRBYTE_USER, AIRBYTE_PASS, PUBLICATION_NAME]):
        print("❌ Error: One or more environment variables are missing from your .env file.")
        return

    print(f"Connecting to database '{DB_NAME}' at {DB_HOST}...")
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        # Autocommit is required for creating users
        conn.autocommit = True
        cursor = conn.cursor()

        # ==========================================
        # 2. CREATE AIRBYTE USER & GRANT PERMISSIONS
        # ==========================================
        print(f"Creating user '{AIRBYTE_USER}' and granting replication permissions...")
        
        # Check if user exists first to avoid errors on re-runs
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (AIRBYTE_USER,))
        if not cursor.fetchone():
            cursor.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s").format(sql.Identifier(AIRBYTE_USER)), [AIRBYTE_PASS])
            print(f"✅ User '{AIRBYTE_USER}' created.")
        else:
            print(f"⚠️ User '{AIRBYTE_USER}' already exists. Skipping creation.")
        
        # Grant standard access to the database schema
        cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(AIRBYTE_USER)))
        cursor.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(sql.Identifier(AIRBYTE_USER)))
        cursor.execute(sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {}").format(sql.Identifier(AIRBYTE_USER)))

        # GRANT REPLICATION (The most important step for CDC!)
        cursor.execute(sql.SQL("GRANT rds_replication TO {}").format(sql.Identifier(AIRBYTE_USER)))
        print("✅ Replication permissions granted.")

        # ==========================================
        # 3. CREATE THE PUBLICATION
        # ==========================================
        print(f"Creating publication '{PUBLICATION_NAME}' for all tables...")
        
        # Check if publication exists
        cursor.execute("SELECT 1 FROM pg_publication WHERE pubname=%s", (PUBLICATION_NAME,))
        if not cursor.fetchone():
            cursor.execute(sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES").format(sql.Identifier(PUBLICATION_NAME)))
            print("✅ Publication created successfully.")
        else:
            print("⚠️ Publication already exists. Skipping.")

        print("\n🚀 Database is fully configured for CDC! You can now set up Airbyte and trigger it with Airflow.")

    except Exception as e:
        print(f"❌ Error during setup: {e}")
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    setup_database_for_cdc()