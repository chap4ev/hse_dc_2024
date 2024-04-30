from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import os

from datasets import load_dataset
import pandas as pd


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'depends_on_past': False,
}


def download_titanic_dataset(path_to_file):
    dataset = load_dataset('lewtun/titanic', split='train')
    df = pd.DataFrame(dataset)
    df.to_csv(path_to_file, index=False)


def calculate_average_age(path_to_file):
    df = pd.read_csv(path_to_file)
    average_age = df['Age'].mean()
    return average_age


def calculate_survival_chance(path_to_file):
    df = pd.read_csv(path_to_file)
    survival_rate = df['Survived'].mean()
    return survival_rate


def save_results(average_age, survival_chance, path_to_file):
    with open(path_to_file, 'w') as f:
        f.write(
            f'Average Age: {average_age}\n'
            f'Survival chance: {survival_chance}'
        )


with DAG(
    'titanic_data_analysis_with_datasets',
    description='Download Titanic dataset with datasets, calculate average age, and save results',
    schedule_interval="@once",
    catchup=False,
    default_args=default_args,
) as dag:
    
    download_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset,
        op_args=['/data/titanic.csv'],
    )

    calculate_age = PythonOperator(
        task_id='calculate_average_age',
        python_callable=calculate_average_age,
        op_args=['/data/titanic.csv'],
    )

    calculate_survival = PythonOperator(
        task_id='calculate_survival_chance',
        python_callable=calculate_survival_chance,
        op_args=['/data/titanic.csv'],
    )

    save_age_results = PythonOperator(
        task_id='save_results',
        python_callable=save_results,
        op_args=[calculate_age.output, calculate_survival.output, '/data/results.txt'],
    )

    download_dataset >> [calculate_age, calculate_survival] >> save_age_results
