Alumno: Rodrigo Ramírez Díaz<br>
ROL: 202273526-0<br>
Repositorio Github tarea 1: https://github.com/rodrigo-ramirezd/Tareas_INF339.git

La actividad se divide en dos secciones.  La primera es la carpeta "workspace/", que incluye los esquemas de avro y parquet, además del código para sus funciones que convierten CSV a Avro o CSV a Parquet. Por otro lado, hay una carpeta "latex/", que guarda los documentos en latex para crear el informe en PDF, así como el tamaño de cada archivo, tanto sin comprimir como comprimido.

## Instrucciones de ejecución:

Para ejecutar el código de la tarea:

1.  Asegúrese de tener Docker instalado y en ejecución (si está utilizando el entorno Docker proporcionado).
2.  Navegue al directorio que contiene el archivo `app.py`. Si está trabajando con el repositorio de GitHub, el comando sería:

    ```bash
    cd Tarea\ 1/workspace/
    ```

3.  Ejecute el script de Python:

    ```bash
    python app.py
    ```

## Prompts a la Herramienta de Inteligencia Artificial

1.- Como funcionan los esquemas csv, avro y parquet?, explicame y muestrame un ejemplo de como se construyen, incluye esquemas de ejmplo si es necesario.

2.- Explicame detalladamente como es el proceso para convertir archivos csv a apache y explicame como se utiliza la libreria fastavro, proporciona ejemplos y material complementario como documentos oficiales.

3.- Porque es necesario tener un type "bytes" en vez de solo decimal?, y explicame la funcion para convertir de decimal a bytes.

4.- Explicame detalladamente como funciona parquet en python, con codigos de ejemplo, esquemas, y lo que consideres necesario para trabajar con parquet en python.

5.- Funcion para leer archivo avro y parquet en python.
