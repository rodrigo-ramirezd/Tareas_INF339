from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions
import json
from datetime import datetime, timedelta
import pendulum

def run_beam_pipeline():
    with beam.Pipeline(options=PipelineOptions()) as p:

        def format_as_json(element):
            if isinstance(element, tuple):
                return element[1].decode('utf-8')
            else:
                raise RuntimeError('unknown record type: %s' % type(element))

        kafka_records = (
            p
            | "ReadFromKafka" >> ReadFromKafka(
                consumer_config={
                    'bootstrap.servers': 'kafka:19092',
                    'auto.offset.reset': 'earliest',
                },
                topics=['etl-events']
            )
            | "FormatAsJson" >> beam.Map(format_as_json)
        )

        (
            kafka_records
            | "Encode to Kafka" >> beam.Map(lambda x: (b'processed', x.encode('utf-8')))
            | "WriteToKafka" >> WriteToKafka(
                producer_config={'bootstrap.servers': 'kafka:19092'},
                topic='processed-events'
            )
        )


with DAG(
    "kafka_beam_pipeline_dag",
    description="Leer de Kafka y escribir en otro topic usando Beam",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 6, 30, tz="UTC"),
    catchup=False,
    tags=["kafka", "beam"],
) as dag:

    start = EmptyOperator(task_id="start")

    beam_task = PythonOperator(
        task_id="run_beam_pipeline",
        python_callable=run_beam_pipeline,
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(seconds=60)
    )

    end = EmptyOperator(task_id="end")

    start >> beam_task >> end
