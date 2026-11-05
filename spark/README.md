# Entrega 2 — Spark

Pipelines de limpieza con Spark 2.4.8 (DataFrames). Se ejecutan en Docker, así que no hace falta instalar Spark, Java ni Python en cada computadora.

## ¿Qué hay aquí?
- **pipeline-examenes.py**: Limpieza del CSV de exámenes (`../datos/datos-grupo.csv`). Guarda el resultado en `datos-salida/examenes_limpios/` y al final muestra consultas sobre los datos limpios (el equivalente en DataFrames de consultas SQL simples).
- **pipeline-salarios.py**: Limpieza del Parquet de salarios (`../datos/salarios-tech.parquet`). Guarda el resultado en `datos-salida/salarios_limpios/` (separado por `;`) y al final muestra consultas sobre los datos limpios.
- **Dockerfile**: Imagen con Spark 2.4.8, Java 8 y Python 3.7.
- **docker-compose.yml**: Comparte con el contenedor la carpeta `datos/` (solo lectura) y esta carpeta.
- **datos-salida/**: Resultados; se crea al correr los pipelines (ignorada por git). Spark guarda cada CSV como una carpeta con un archivo `part-00000-….csv` adentro.

## Pasos para ejecutarlo
Solo necesitas Docker Desktop encendido.

1. Abre una terminal en esta carpeta (`spark/`).
2. Construye la imagen (la primera vez, o si cambia el `Dockerfile`):
   `docker compose build`
3. Corre el pipeline de exámenes:
   `docker compose run --rm spark`
4. Corre el pipeline de salarios:
   `docker compose run --rm spark pipeline-salarios.py`

Editar un `.py` no requiere reconstruir la imagen.

## ¿Por qué estas versiones?
La entrega pide Spark 2. Spark 2.4 solo funciona con Java 8, y PySpark 2.4 no funciona con Python 3.8 o superior ([SPARK-29536](https://issues.apache.org/jira/browse/SPARK-29536)).

## Notas
- En salarios, las filas no salen en el orden del archivo original, porque `dropDuplicates()` de Spark no conserva el orden.
- En Spark 2.4, leer una columna con espacios en el nombre (como `Unnamed: 0` en `salarios-tech.parquet`) falla ([SPARK-27442](https://issues.apache.org/jira/browse/SPARK-27442)). Por eso `pipeline-salarios.py` selecciona solo las columnas necesarias justo al leer el archivo.
- Los dos pipelines configuran `spark.sql.shuffle.partitions` en 4. Con el valor por defecto (200), Spark 2.4 fallaba a veces con un `ClassCastException` en computadoras con muchos núcleos: 4 de 10 corridas en una de 32 núcleos. Con 4 particiones no falló en ninguna de 25 corridas.
