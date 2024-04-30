from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


def process_numbers(ti):
    random_numbers = ti.xcom_pull(task_ids='fetch_random_numbers')
    numbers_list = random_numbers.split()
    total_sum = sum(int(num) for num in numbers_list)
    return total_sum


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'depends_on_past': False,
}


with DAG(
    dag_id="fetch_and_process_random_numbers",
    description="Fetch random numbers from random.org and process them",
    schedule_interval="@once",
    catchup=False,
    default_args=default_args,
) as dag:
    fetch_random_numbers = SimpleHttpOperator(
        task_id='fetch_random_numbers',
        method='GET',
        http_conn_id='random_org',
        endpoint='integers/?num=10&min=1&max=100&col=1&base=10&format=plain&rnd=new',
        response_filter=lambda response: response.text,
    )

    process_random_numbers = PythonOperator(
        task_id='process_random_numbers',
        python_callable=process_numbers,
        provide_context=True,
    )

    display_result = BashOperator(
        task_id='display_result',
        bash_command='echo "Total sum of random numbers is: {{ task_instance.xcom_pull(task_ids=\'process_random_numbers\') }}"',
    )

    fetch_random_numbers >> process_random_numbers >> display_result
