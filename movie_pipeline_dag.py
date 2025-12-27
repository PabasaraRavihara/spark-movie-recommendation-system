from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'chamuditha',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'movie_recommendation_pipeline',
    default_args=default_args,
    description='A simple Spark ML pipeline',
    schedule_interval='0 8 * * *',  # හැමදාම උදේ 8.00 ට (Cron Expression)
)



check_data = BashOperator(
    task_id='check_data_availability',
    bash_command='ls /home/data/ratings.csv',
    dag=dag,
)


run_spark_job = BashOperator(
    task_id='run_spark_ml_job',
    bash_command='spark-submit movie_recommendation_engine.py', 
    dag=dag,
)


check_data >> run_spark_job