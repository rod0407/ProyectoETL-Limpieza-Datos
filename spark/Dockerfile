# Imagen para correr los pipelines de la entrega 2 con Spark 2.4.8 (la última versión 2.x).
# - Spark 2.4.8 solo funciona con Java 8.
# - PySpark 2.4 no funciona con Python 3.8 o superior, por eso se usa Python 3.7.
# El código y los datos no se copian a la imagen: docker-compose.yml los comparte con el contenedor.

# Java 8 se toma de la imagen oficial de Eclipse Temurin.
FROM eclipse-temurin:8-jre AS java

FROM python:3.7-slim-bullseye

COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=/opt/java/openjdk/bin:$PATH

# Instala PySpark 2.4.8 (trae Spark incluido). --no-cache-dir evita guardar el instalador dentro de la imagen.
RUN pip install --no-cache-dir pyspark==2.4.8

# El contenedor arranca dentro de spark/ y ejecuta el script con spark-submit.
WORKDIR /app/spark
ENTRYPOINT ["spark-submit"]
CMD ["pipeline-examenes.py"]
