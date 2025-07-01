import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import WriteToKafka
import json
from datetime import datetime
import fastavro


def parse_json(line):
    data = json.loads(line)
    ts_format = '%Y-%m-%d %H:%M:%S'
    dt = datetime.strptime(data['Timestamp'], ts_format)
    data['Timestamp_unix'] = int(dt.timestamp() * 1000)
    return data


def run():
    input_path = './data/input/fan_engagement.json'
    output_path = './data/output/result.avro'
    schema_path = './beam_pipeline/fan_engagement.avsc'

    with open(schema_path, 'r') as f:
        schema = fastavro.parse_schema(json.load(f))

    with beam.Pipeline(options=PipelineOptions()) as p:

        # Read and parse JSONL file
        parsed_records = (
            p
            | 'Read JSONL' >> beam.io.ReadFromText(input_path)
            | 'Parse and Transform' >> beam.Map(parse_json)
        )

        # Write to Avro
        _ = (
            parsed_records
            | 'Write Avro' >> beam.io.WriteToAvro(
                file_path_prefix=output_path.replace('.avro', ''),
                schema=schema,
                file_name_suffix='.avro',
                use_fastavro=True
            )
        )

        # Create Kafka message
        _ = (
            p
            | 'Create Kafka Tuple' >> beam.Create([
                (b'notification', json.dumps({
                    'event_type': 'data_processing_completed',
                    'data_entity': 'FanEngagement',
                    'status': 'success',
                    'location': output_path,
                    'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'source_system': 'fan_engagement_dag'
                }).encode('utf-8'))
            ])
            | 'Send to Kafka' >> beam.io.kafka.WriteToKafka(
                producer_config={
                    'bootstrap.servers': 'kafka:19092'
                },
                topic='etl-events'
            )
        )



if __name__ == '__main__':
    run()
