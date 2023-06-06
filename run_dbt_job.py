from datetime import datetime
import os

from airflow.models import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

DAG_ID = "Execute_DBT_Job"
ACC_ID = os.environ.get("Account_ID")
JOB_ID = os.environ.get("Job_ID")

with DAG(
    dag_id=DAG_ID,
    default_args={"dbt_cloud_conn_id": "dbt", "account_id": ACC_ID},
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

   
    trigger_job_run1 = DbtCloudRunJobOperator(
        task_id="trigger_dbt_job",
        job_id=JOB_ID,
        check_interval=10,
        timeout=300,
    )

    begin>>trigger_job_run1>>end