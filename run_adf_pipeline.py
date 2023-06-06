from datetime import datetime

from airflow.models import DAG
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

DAG_ID = "Execute_ADF_Pipeline"

with DAG(
    dag_id=DAG_ID,
    default_args={"azure_data_factory_conn_id": "azure_data_factory"},
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

   
    run_pipeline1 = AzureDataFactoryRunPipelineOperator(
        task_id="Execute_ADF_Pipeline",
        pipeline_name="DBT_Trigger_Job"
    )

    begin>>run_pipeline1>>end