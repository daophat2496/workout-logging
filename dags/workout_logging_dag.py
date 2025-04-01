import os
import dlt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from cosmos import DbtTaskGroup, ProjectConfig, ExecutionConfig, ProfileConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from dlt.helpers.airflow_helper import PipelineTasksGroup
from dlt.common import pendulum
import pandas as pd
from datetime import datetime, timedelta

# Importing custom modules
from dlt_ingestion.strava_source import load_strava_activities
from include.helpers import set_dlt_env_vars

# Environment Variables
DBT_EXECUTABLE_PATH = os.path.join(os.environ['AIRFLOW_HOME'], "dbt_venv/bin/dbt")
DBT_PROJECT_PATH = os.path.join(os.environ['AIRFLOW_HOME'], "dbt_transform")
BQ_STRAVA_RAW_DATASET = os.environ['BQ_STRAVA_RAW_DATASET']
BQ_TABLE = "activities"
GCP_CONN_ID = "GCP"

# Default Task Arguments
default_task_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0
}

# Function to extract the latest timestamp from BigQuery
def extract_latest_timestamp(**kwargs):
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    
    # First check if dataset exists
    try:
        query = f"""
            SELECT MAX(start_date) as latest_timestamp 
            FROM `{BQ_STRAVA_RAW_DATASET}.{BQ_TABLE}`
        """
        records = bq_hook.get_pandas_df(query)
    except Exception as e:
        if "Not found: Dataset" in str(e) or "Not found: Table" in str(e):
            # Dataset or table doesn't exist yet
            print(f"Dataset/Table not found, using default timestamp. Error: {str(e)}")
            kwargs['ti'].xcom_push(key='latest_timestamp', value=None)
            return
        else:
            # Other unexpected error - re-raise
            raise
    
    latest_timestamp = records["latest_timestamp"].iloc[0] if not records.empty else None
    
    if pd.notna(latest_timestamp):
        if isinstance(latest_timestamp, pd.Timestamp):
            latest_timestamp = latest_timestamp.to_pydatetime().isoformat()
        else:
            latest_timestamp = latest_timestamp.isoformat()
    
    kwargs['ti'].xcom_push(key='latest_timestamp', value=latest_timestamp)

# DAG Definition
with DAG(
    dag_id="workout_logging_dag",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_task_args
) as dag:
    
    # Set GCP Environment Variables
    gcp_hook = GoogleBaseHook(gcp_conn_id=GCP_CONN_ID)
    set_dlt_env_vars(gcp_hook)

    # Extract Latest Timestamp Task
    extract_latest_timestamp_task = PythonOperator(
        task_id="extract_latest_timestamp",
        python_callable=extract_latest_timestamp,
        provide_context=True
    )

    # Data Ingestion Task
    def ingest_with_latest_timestamp(**kwargs):
        latest_timestamp = kwargs['ti'].xcom_pull(task_ids='extract_latest_timestamp', key='latest_timestamp')
        
        if latest_timestamp:
            latest_timestamp = datetime.fromisoformat(latest_timestamp)  # Convert back to datetime
        else:
            latest_timestamp = datetime(2024, 2, 1)  # Default timestamp
        
        load_strava_activities(
            latest_timestamp=latest_timestamp,
            bigquery_dataset_name=BQ_STRAVA_RAW_DATASET
        )

    ingest_strava_data = PythonOperator(
        task_id="ingest_strava_data",
        python_callable=ingest_with_latest_timestamp,
        provide_context=True
    )

    # DBT Configuration
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
            conn_id=GCP_CONN_ID,
            profile_args={
                "dataset": "dbt_transform",
                "location": "asia-southeast1"
            }
        )
    )

    execution_config = ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH
    )

    # DBT Task Group
    dbt_run = DbtTaskGroup(
        group_id="dbt_run",
        profile_config=profile_config,
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        execution_config=execution_config
    )

    # Task Dependencies
    extract_latest_timestamp_task >> ingest_strava_data >> dbt_run
