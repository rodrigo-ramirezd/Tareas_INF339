import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.avroio import WriteToAvro 
import json
from datetime import datetime
import argparse
import logging

def convert_timestamp(data):
    """Convierte Timestamp en formato string a Unix Timestamp en milisegundos"""
    dt = datetime.strptime(data['Timestamp'], '%Y-%m-%d%H:%M:%S')
    data['Timestamp_unix'] = int(dt.timestamp() * 1000)
    return data


def safe_parse_json(line):
    try:
        return json.loads(line)
    except Exception as e:
        logging.warning(f"Error al parsear línea JSON: {e}")
        return None

def run_pipeline(input_path, output_path, schema_path):
    #Leer el esquema Avro como dict
    with open(schema_path, 'r') as f:
        schema = json.load(f)

    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Leer líneas de JSON' >> beam.io.ReadFromText(input_path)
            | 'Parsear JSON' >> beam.Map(safe_parse_json)
            | 'Filtrar líneas inválidas' >> beam.Filter(lambda x: x is not None)
            | 'Convertir timestamp' >> beam.Map(convert_timestamp)
            | 'Filtrar errores de timestamp' >> beam.Filter(lambda x: x is not None)
            | 'Escribir a AVRO' >> WriteToAvro(
                file_path_prefix=output_path,
                schema=schema,
                file_name_suffix='.avro',
                codec='deflate'
            )
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--schema', default='schemas/fan_engagement.avsc')
    args = parser.parse_args()

    run_pipeline(args.input, args.output, args.schema)
