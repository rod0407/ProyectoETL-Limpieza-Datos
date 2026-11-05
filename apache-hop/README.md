# Entrega 1 — Apache Hop

Proyecto colaborativo para limpiar datos con Apache Hop y cargarlos en MySQL.

## ¿Qué hay aquí?
- **../datos/**: Datos de entrada compartidos con las demás entregas (`datos-grupo.csv` y `salarios-tech.parquet`).
- **datos-salida/**: CSV procesados que generan los pipelines (se crea al correrlos; ignorada por git).
- **pipeline_examenes.hpl**: El flujo de Apache Hop del CSV.
- **pipeline_salarios.hpl**: El flujo de Apache Hop del Parquet (salarios tech).
- **docker-compose.yml**: Archivo para levantar la base de datos local rápidamente.
- **db/init/**: Scripts que crean las tablas al levantar la base de datos.
- **metadata/** y **project-config.json**: Proyecto de Hop con la conexión `mysql_proyecto_etl` ya configurada.

## Pasos para ejecutarlo
1. Abre una terminal en esta carpeta (`apache-hop/`) y levanta la base de datos con:
   `docker compose up -d`
2. Abre Apache Hop y agrega esta carpeta como proyecto (**Add project**, home = carpeta `apache-hop/` del repo).
   - Para el pipeline del Parquet instala el plugin de Parquet desde el Marketplace de Hop y copia [`hadoop-shaded-guava-1.4.0.jar`](https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.4.0/hadoop-shaded-guava-1.4.0.jar) en `lib\core\` de Hop.
   - Copia el driver [`mysql-connector-j-26.7.0.jar`](https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/26.7.0/mysql-connector-j-26.7.0.jar) en `lib\jdbc\` de Hop.
3. Abre el `.hpl` que quieras correr. Las rutas usan `${PROJECT_HOME}` (entradas en `${PROJECT_HOME}/../datos/`), ya no hay que cambiarlas.
4. Corre el pipeline.

## Si ya tenías el proyecto antes de la reorganización
- En Hop, edita el proyecto y cambia su home de la raíz del repo a la carpeta `apache-hop/` (o elimínalo y vuelve a agregarlo).
- Si tenías la base levantada desde la raíz, bórrala antes de levantarla desde `apache-hop/`, porque el contenedor `etl_db` ya existe: `docker rm -f etl_db`.

## Siguientes pasos (Pendientes)
- [x] Agregar nodo para pasar textos a mayúsculas.
- [x] Agregar nodo para limpiar acentos.
