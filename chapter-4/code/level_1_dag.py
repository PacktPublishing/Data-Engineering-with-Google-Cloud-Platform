from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'packt-developer',
}

with DAG(
    dag_id='hello_world_airflow',
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=days_ago(1),
) as dag:

    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo Hello',
    )

    print_world= BashOperator(
        task_id='print_world',
        bash_command='echo World',
    )

    print_hello >> print_world

if __name__ == "__main__":
    dag.cli()
