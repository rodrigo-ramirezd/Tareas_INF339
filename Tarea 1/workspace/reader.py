import pyarrow.parquet as pq
import fastavro

# Leer un archivo Parquet
try:
    table = pq.read_table('./output/EarthquakesChile_2000-2024_1pct.csv.NONE.parquet')
    df = table.to_pandas()
    print("Datos de Parquet:")
    print(df)
except FileNotFoundError:
    print("Error: El archivo Parquet 'mi_archivo.parquet' no se encontró.")

# Leer un archivo Avro
try:
    with open('./output/EarthquakesChile_2000-2024_1pct.csv.null.avro', 'rb') as avro_file:
        reader = fastavro.reader(avro_file)
        for record in reader:
            print("Registro Avro:")
            print(record)
except FileNotFoundError:
    print("Error: El archivo Avro 'mi_archivo.avro' no se encontró.")