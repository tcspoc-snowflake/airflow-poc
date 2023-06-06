from datetime import datetime

from airflow.models import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

DAG_ID = "Execute_Databricks_Notebook"

with DAG(
    dag_id=DAG_ID,
    default_args={"databricks_conn_id": "databricks_default"},
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

    notebook_task_params_2 = {
        "existing_cluster_id": "0515-060723-addqdawg",
        "notebook_task": {
            "notebook_path": "/Delta_snowflake/Snowflake_connect",
        },
    }

    notebook_task_params_3 = {
        "existing_cluster_id": "0515-060723-addqdawg",
        "notebook_task": {
            "notebook_path": "/Delta_snowflake/DDL_operations_on_delta",
        },
    }

    notebook_task_1 = DatabricksSubmitRunOperator(task_id="ADLSGen2-Snowflake-DeltaTable_delta_format", json=notebook_task_params_1)
    notebook_task_2 = DatabricksSubmitRunOperator(task_id="Snowflake_connect", json=notebook_task_params_2)
    notebook_task_3 = DatabricksSubmitRunOperator(task_id="DDL_operations_on_delta", json=notebook_task_params_3)

    begin>>notebook_task_1>>notebook_task_2>>notebook_task_3>>end