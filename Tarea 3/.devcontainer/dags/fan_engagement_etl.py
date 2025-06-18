from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import json
from confluent_kafka import Producer
import logging

def notify_kafka(**context):
    conf = {'bootstrap.servers': 'kafka:9092'}
    producer = Producer(conf)
    
    output_path = context['ti'].xcom_pull(task_ids='run_beam_job')
    
    message = {
        "event_type": "data_processing_completed",
        "data_entity": "FanEngagement",
        "status": "success",
        "location": output_path,
        "processed_at": datetime.now().strftime('%Y-%m-%d%H:%M:%S'),
        "source_system": "fan_engagement_etl"
    }
    
    producer.produce('data_processing', value=json.dumps(message))
    producer.flush()
    logging.info(f"Notification sent to Kafka: {message}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fan_engagement_etl',
    default_args=default_args,
    description='ETL diaria para datos de participación de fans HRL',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

run_etl = BashOperator(
    task_id='run_beam_job',
    bash_command='python /workspace/src/beam_etl.py --input /data/input/{{ ds }}.json --output /data/output/{{ ds }}.avro && echo "/data/output/{{ ds }}.avro"',
    do_xcom_push=True,
    dag=dag,
)

send_notification = PythonOperator(
    task_id='send_kafka_notification',
    python_callable=notify_kafka,
    dag=dag,
)

run_etl >> send_notification
