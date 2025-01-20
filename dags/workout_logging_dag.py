import dlt
from airflow import DAG
from dlt.common import pendulum
from dlt.helpers.airflow_helper import PipelineTasksGroup
from airflow.operators.python_operator import PythonOperator
import os
from cosmos import DbtTaskGroup, ProjectConfig, ExecutionConfig, ProfileConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_transform"

default_task_args = {
    "owner": "airflow"
    , "depends_on_past": False
    , "email": "daophat2496@gmnail.com"
    , "retries": 0
}

with DAG(
    dag_id = "workout_logging_dag"
    , schedule=None
    , start_date=pendulum.datetime(2025, 1, 1)
    , catchup=False
    , max_active_runs=1
    , default_args=default_task_args
):

    from dlt_ingestion.strava_source import load_strava_activities

    ingest_strava_data = PythonOperator(
        task_id="ingest_strava_data"
        , python_callable=load_strava_activities
    )

    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
            conn_id = "GCP"
            , profile_args = {
                "dataset": "dbt_transform"
                , "location": "asia-southeast1"
            }
        )
    )

    execution_config = ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH
    )

    dbt_run = DbtTaskGroup(
        group_id="dbt_run"
        , profile_config=profile_config
        , project_config=ProjectConfig(DBT_PROJECT_PATH)
        , execution_config=execution_config
    )

    ingest_strava_data >> dbt_run