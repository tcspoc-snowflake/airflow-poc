from datetime import datetime
import os

from airflow.models import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

DAG_ID = "Execute_Databricks_DBT_Setup_Job"
ACC_ID = os.environ.get("Account_ID")
JOB_ID = os.environ.get("Job_ID")

with DAG(
    dag_id=DAG_ID,
    default_args={"databricks_conn_id": "databricks_default", "dbt_cloud_conn_id": "dbt", "account_id": ACC_ID},
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    notebook_task_params_1 = {
        "existing_cluster_id": "0515-060723-addqdawg",
        "notebook_task": {
            "notebook_path": "/Delta_snowflake/ADLSGen2-Snowflake-DeltaTable_delta_format",
        },
    }

    notebook_task_1 = DatabricksSubmitRunOperator(task_id="ADLSGen2-Snowflake-DeltaTable_delta_format", json=notebook_task_params_1)

    trigger_job_run1 = DbtCloudRunJobOperator(
        task_id="Trigger_DBT_QA_Job",
        job_id=JOB_ID,
        check_interval=10,
        timeout=300,
    )

    begin>>notebook_task_1>>trigger_job_run1>>end



    