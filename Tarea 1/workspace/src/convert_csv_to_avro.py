import csv
import fastavro
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path
from fastavro.schema import load_schema
from .convert import Convert


def decimal_to_bytes(value: Decimal, scale: int) -> bytes:
    value = value.quantize(Decimal((0, (1,), -scale)))
    unscaled = int(value * (10 ** scale))
    num_bytes = (unscaled.bit_length() + 8) // 8 or 1
    return unscaled.to_bytes(num_bytes, byteorder='big', signed=True)


class ConvertCsvToAvro(Convert):

    def convert(self, input_filename, output_folder, compression):
        output_path = self.get_output_filename(input_filename, output_folder, "avro", compression)
        print(f"Converting {input_filename} to AVRO with compression {compression}")
        
        parsed_schema = load_schema('resources/Earthquake schema.avsc')

        records = []
        with open(input_filename, encoding='utf-8', newline='') as csvfile:
            datareader = csv.DictReader(csvfile, delimiter=',')
            for row in datareader:
                try:
                    record = {
                        "UTC-Date": int(datetime.strptime(row['UTC_Date'], '%Y-%m-%d %H:%M:%S').timestamp() * 1000),
                        "Profundity": row.get("Profundity") or None,
                        "Magnitude": row.get("Magnitude") or None,
                        "Date": (datetime.strptime(row['Date'], '%Y-%m-%d').date() - date(1970, 1, 1)).days,
                        "Hour": (
                            int(
                                datetime.strptime(row['Hour'], '%H:%M:%S').hour * 3600000 +
                                datetime.strptime(row['Hour'], '%H:%M:%S').minute * 60000 +
                                datetime.strptime(row['Hour'], '%H:%M:%S').second * 1000
                            )
                        ),
                        "Location": row.get("Location") or None,
                        "Latitude": decimal_to_bytes(Decimal(row.get('Latitude', '0.0')), scale=3),
                        "Longitude": decimal_to_bytes(Decimal(row.get('Longitude', '0.0')), scale=3),
                    }
                    records.append(record)
                except Exception as e:
                    print(f"Error processing row {row}: {e}")
        
        with open(output_path, 'wb') as out:
            fastavro.writer(out, parsed_schema, records, codec=compression)

        print(f"AVRO file created successfully at {output_path}")
        return output_path
