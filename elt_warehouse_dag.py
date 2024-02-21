from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from includes.database_connection import etl_to_warehouse



dag = DAG(
    'etl_warehouse_dag',    
    start_date=datetime(2024, 21, 2),
    description='ETL process into a warehouse',
    schedule_interval='0 15 * * *',
    catchup=False
)


start_task = DummyOperator(task_id='start_task', dag=dag)

etl_task = PythonOperator(
    task_id='etl_process',
    python_callable=etl_to_warehouse,
    dag=dag,
)

end_task = DummyOperator(task_id='start_task', dag=dag)

start_task >> etl_task >> end_task