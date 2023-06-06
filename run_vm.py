from datetime import datetime

from airflow.models import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

DAG_ID = "Execute_Shell_Cmd"

def getOutput(**kwargs):
    task_instance = kwargs['task_instance']
    value = {{task_instance.xcom_pull(task_ids='Execute_Command')}}
    print(value)

with DAG(
    dag_id=DAG_ID,
    default_args={"ssh_conn_id": "ssh_default"},
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    command = """
    echo 'Hello World'
    """

    run_command = SSHOperator(
        task_id='Execute_Command',
        command=command,
        do_xcom_push=True,
        )
    
    # check_output = PythonOperator(
    #     task_id='Get_output',
    #     python_callable = getOutput,
    #     provide_context = True
    # )
    
    begin>>run_command>>end