from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

AIRFLOW_HOME = '/home/lucas/projetos/Aula_Cloud_Aplicada/aula_standalone/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_message():
    print("Leitura realizada")

def print_error():
    print("Não foi possível ler o Banco de Dados")    

def read_alunos():
    pghook = PostgresHook(postgres_conn_id='CONEXAO_AULAS')
    pghook.copy_expert(
            "COPY (select * from alunos) TO stdout WITH CSV HEADER",
            AIRFLOW_HOME + 'data/alunos.csv'
    )
    
def decide_branch(ti):
    result = ti.xcom_pull(task_ids='task_bash')
    if result == "Arquivos gerados":
        return 'task_query_alunos'
    else:
        return 'task_error'

dag = DAG(
    dag_id='atividade_1',
    default_args=default_args,
    description='Atividade Prática 1',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

print_message_task = PythonOperator(
    task_id='task_PythonOp',
    python_callable=print_message,
    dag=dag,
)

list_files_task = BashOperator(
    task_id='task_bash',
    bash_command="echo 'Arquivos gerados'",
    dag=dag,
)

query_postgres_task = PythonOperator(
    task_id='task_query_alunos',
    python_callable=read_alunos,
    provide_context=True,
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id='task_decide_branch',
    python_callable=decide_branch,
    provide_context=True,
    dag=dag,
)

task_error = PythonOperator(
    task_id='task_error',
    python_callable=print_error,
    dag=dag
)

print_message_task >> list_files_task
list_files_task >> branch_task
branch_task >> [query_postgres_task, task_error]