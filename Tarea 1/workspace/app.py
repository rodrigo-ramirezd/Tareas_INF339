from src.convert_csv_to_avro import ConvertCsvToAvro
from src.convert_csv_to_parquet import ConvertCsvToParquet
from pathlib import Path

def main():
  print("Running")
  files = [
    "data/EarthquakesChile_2000-2024_1pct.csv",
    "data/EarthquakesChile_2000-2024_10pct.csv",
    "data/EarthquakesChile_2000-2024_25pct.csv",
    "data/EarthquakesChile_2000-2024_50pct.csv",
    "data/EarthquakesChile_2000-2024.csv"
  ]
  for file in files:
    print(f"\n--------------------------------------------\n\n{file}")
    ConvertCsvToAvro().convert(file, "output", "null")
    ConvertCsvToAvro().convert(file, "output", "deflate")
    ConvertCsvToAvro().convert(file, "output", "snappy")
    ConvertCsvToParquet().convert(file, "output", "NONE")
    ConvertCsvToParquet().convert(file, "output", "SNAPPY")
    ConvertCsvToParquet().convert(file, "output", "GZIP")
    ConvertCsvToParquet().convert(file, "output", "LZ4")
  
  print("\n--------------------------------------------\n\nFile Sizes:\n")
  
  # List all files in the output directory and print their sizes
  output_dir = Path("output/")
  for file in output_dir.iterdir():
    size = Path(file).stat().st_size / (1024 * 1024)
    print(f"File: {file} Size: {size:.2f} MB")

if __name__ == "__main__": 
  main()