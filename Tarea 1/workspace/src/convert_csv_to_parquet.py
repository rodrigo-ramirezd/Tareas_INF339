import csv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date
from decimal import Decimal
from .convert import Convert

class ConvertCsvToParquet(Convert):

    def convert(self, input_filename, output_folder, compression):
        # Get output filename
        output_path = self.get_output_filename(input_filename, output_folder, "parquet", compression)
        print(f"Converting {input_filename} to Parquet with compression {compression}")

        # Create schema
        pa.schema([
            ("UTC_Date", pa.timestamp('ms')),
            ("Profundity", pa.string()),
            ("Magnitude", pa.string()),
            ("Date", pa.date32()),
            ("Hour", pa.time32('ms')),
            ("Location", pa.string()),
            ("Latitude", pa.decimal128(5, 3)),
            ("Longitude", pa.decimal128(6, 3)),
        ])

        # Reading CSV and converting to Parquet
        records = []
        with open(input_filename, encoding='utf-8', newline='') as csvfile:
            datareader = csv.DictReader(csvfile, delimiter=',')
            for row in datareader:
                try:
                    utc_date = int(datetime.strptime(row["UTC_Date"], "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
                    date_val = (datetime.strptime(row["Date"], "%Y-%m-%d").date() - date(1970, 1, 1)).days
                    time_val = datetime.strptime(row["Hour"], "%H:%M:%S")
                    hour_ms = (time_val.hour * 3600 + time_val.minute * 60 + time_val.second) * 1000

                    records.append({
                        "UTC_Date": utc_date,
                        "Profundity": row["Profundity"],
                        "Magnitude": row["Magnitude"],
                        "Date": date_val,
                        "Hour": hour_ms,
                        "Location": row["Location"],
                        "Latitude": Decimal(row["Latitude"]),
                        "Longitude": Decimal(row["Longitude"]),
                    })
                except Exception as e:
                    print(f"Error en fila: {row}")
                    print(f"Motivo: {e}")
            
        # Save the Parquet file
        with open(output_path, 'wb') as out:
            table = pa.Table.from_pylist(records)
            pq.write_table(table, out, compression=compression)

        print(f"Parquet file created successfully at {output_path}")
        return output_path
