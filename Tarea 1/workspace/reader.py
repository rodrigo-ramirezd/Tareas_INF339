# filepath: /workspaces/Homeworks/Tarea 1/workspace/reader.py
import os
import fastavro

file_path = '/workspaces/Homeworks/Tarea 1/workspace/output/EarthquakesChile_2000-2024_1pct.csv.deflate.avro'

if not os.path.exists(file_path):
    print(f"Error: File not found: {file_path}")
else:
    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        for record in reader:
            print(record)