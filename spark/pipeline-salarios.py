from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, regexp_replace


# Inicializar Spark (igual que en el pipeline de examenes)
# SparkConf: la configuracion, aqui le ponemos nombre a la aplicacion y el numero de particiones
# SparkContext: la conexion con Spark, se crea con la configuracion
# SparkSession: se crea con el SparkContext y es la que usamos para leer archivos y trabajar con DataFrames

# spark.sql.shuffle.partitions: cuando Spark agrupa o quita repetidos reparte los datos en 200 partes por defecto
# como son pocos datos con 4 partes basta, y con 200 a veces Spark 2 fallaba en compus con muchos nucleos
conf = SparkConf().setAppName("LimpiezaSalarios").set("spark.sql.shuffle.partitions", "4")
sc = SparkContext(conf=conf)
# se agrego porque los mensajes INFO de Spark no dejaban ver la salida de las consultas, asi solo muestra advertencias y errores
sc.setLogLevel("WARN")
spark = SparkSession(sc)


# ruta del archivo, el programa se ejecuta desde la carpeta spark/ por eso se usa ".." para subir a la carpeta del proyecto
ruta_entrada = "../datos/salarios-tech.parquet"
print(f"[INFO] Cargando archivo: {ruta_entrada}")

# Se lee el archivo parquet y se guarda en el DataFrame df
df = spark.read.format("parquet").load(ruta_entrada)


# seleccionar columnas
# select(): se queda solo con las columnas que se escriben
# el archivo tiene una columna extra "Unnamed: 0" que no se usa
# ademas Spark 2 marca error si intentas leer esa columna porque su nombre tiene un espacio, por eso se quita desde aqui

df = df.select("work_year", "experience_level", "employment_type", "job_title", "salary", "salary_currency",
               "salary_in_usd", "employee_residence", "remote_ratio", "company_location", "company_size")

print("\nESTRUCTURA ORIGINAL")
print(f"Total de filas: {df.count()}")
df.printSchema()

# withColumnRenamed(): cambia el nombre de una columna
df_limpio = df.withColumnRenamed("work_year", "anio")
df_limpio = df_limpio.withColumnRenamed("experience_level", "nivel_experiencia")
df_limpio = df_limpio.withColumnRenamed("employment_type", "tipo_empleo")
df_limpio = df_limpio.withColumnRenamed("job_title", "puesto")
df_limpio = df_limpio.withColumnRenamed("salary", "salario")
df_limpio = df_limpio.withColumnRenamed("salary_currency", "moneda")
df_limpio = df_limpio.withColumnRenamed("salary_in_usd", "salario_usd")
df_limpio = df_limpio.withColumnRenamed("employee_residence", "pais_residencia")
df_limpio = df_limpio.withColumnRenamed("remote_ratio", "modalidad")
df_limpio = df_limpio.withColumnRenamed("company_location", "pais_empresa")
df_limpio = df_limpio.withColumnRenamed("company_size", "tamano_empresa")

# modalidad viene como numero 0, 50 o 100, se cambia a texto para despues ponerle nombre
# cast("string"): convierte la columna a texto
df_limpio = df_limpio.withColumn("modalidad", col("modalidad").cast("string"))

df_limpio.show()


# quitar filas repetidas
# dropDuplicates(): borra las filas que son iguales en todas sus columnas y deja solo una
df_limpio = df_limpio.dropDuplicates()

print(f"Filas despues de quitar repetidos: {df_limpio.count()}")


# cambiar las claves por su significado
# replace(lista de valores a buscar, lista de valores nuevos, columna)
# el primer valor de la lista se cambia por el primero de la otra lista, el segundo por el segundo, etc.
# si un valor no esta en la lista se queda igual

# nivel de experiencia: EN = junior, MI = intermedio, SE = senior, EX = ejecutivo
df_limpio = df_limpio.replace(["EN", "MI", "SE", "EX"], ["JUNIOR", "INTERMEDIO", "SENIOR", "EJECUTIVO"], "nivel_experiencia")

# tipo de empleo: FT = full time, PT = part time, CT = contrato, FL = freelance
df_limpio = df_limpio.replace(["FT", "PT", "CT", "FL"], ["TIEMPO COMPLETO", "MEDIO TIEMPO", "CONTRATO", "FREELANCE"], "tipo_empleo")

# tamaño de la empresa: S = pequeña, M = mediana, L = grande
df_limpio = df_limpio.replace(["S", "M", "L"], ["PEQUENA", "MEDIANA", "GRANDE"], "tamano_empresa")

# modalidad: es el porcentaje de trabajo remoto, 0 = presencial, 50 = hibrido, 100 = remoto
df_limpio = df_limpio.replace(["0", "50", "100"], ["PRESENCIAL", "HIBRIDO", "REMOTO"], "modalidad")

df_limpio.show()


# limpiar el puesto
# trim(): quita los espacios al inicio y al final
# upper(): pasa el texto a mayusculas
df_limpio = df_limpio.withColumn("puesto", trim(col("puesto")))
df_limpio = df_limpio.withColumn("puesto", upper(col("puesto")))


# corregir nombres de puestos
# regexp_replace(columna, texto a buscar, texto nuevo): igual que en el pipeline de examenes
df_limpio = df_limpio.withColumn("puesto", regexp_replace(col("puesto"), "ML ENGINEER", "MACHINE LEARNING ENGINEER"))
df_limpio = df_limpio.withColumn("puesto", regexp_replace(col("puesto"), "FINANCE DATA ANALYST", "FINANCIAL DATA ANALYST"))

df_limpio.show()


# filtrar filas
# where(): deja solo las filas que cumplen la condicion
# se deja el salario mayor a 0 y el año 2020 o despues
df_limpio = df_limpio.where(col("salario_usd") > 0)
df_limpio = df_limpio.where(col("anio") >= 2020)


#RESULTADO FINAL
print("\nREGISTROS LIMPIOS")
print(f"Total de filas: {df_limpio.count()}")
df_limpio.show()

print("\nESQUEMA FINAL")
df_limpio.printSchema()


#GUARDAR EL ARCHIVO DE SALIDA
# igual que en examenes, Spark guarda una carpeta con un archivo part-00000-....csv adentro
# coalesce(1): junta todo en una sola parte para que quede un solo archivo csv
# sep = ";": separa las columnas con punto y coma
ruta_salida = "datos-salida/salarios_limpios"

df_limpio.coalesce(1).write \
    .format("csv") \
    .option("header", "true") \
    .option("sep", ";") \
    .mode("overwrite") \
    .save(ruta_salida)

print("Archivo guardado correctamente")


# CONSULTAS
# lo mismo que haríamos con SQL pero con funciones de DataFrame, arriba de cada una esta la consulta en SQL

# SQL: SELECT * FROM salarios LIMIT 5
print("\nCONSULTA 1: primeras 5 filas")
df_limpio.show(5)

# SQL: SELECT COUNT(*) FROM salarios
print("\nCONSULTA 2: total de registros")
print(df_limpio.count())

# SQL: SELECT puesto, salario_usd FROM salarios
print("\nCONSULTA 3: puesto y salario en dolares")
df_limpio.select("puesto", "salario_usd").show()

# SQL: SELECT puesto, salario_usd, modalidad FROM salarios WHERE modalidad = 'REMOTO'
# en Spark se compara con == (doble igual)
print("\nCONSULTA 4: solo los trabajos remotos")
df_limpio.select("puesto", "salario_usd", "modalidad").where(col("modalidad") == "REMOTO").show()

# SQL: SELECT nivel_experiencia, COUNT(*) FROM salarios GROUP BY nivel_experiencia
# groupBy(): junta las filas con el mismo valor, count(): cuenta cuantas hay en cada grupo
print("\nCONSULTA 5: cuantos hay de cada nivel de experiencia")
df_limpio.groupBy("nivel_experiencia").count().show()

# SQL: SELECT nivel_experiencia, AVG(salario_usd) FROM salarios GROUP BY nivel_experiencia
# avg(): saca el promedio de la columna en cada grupo
print("\nCONSULTA 6: salario promedio en dolares por nivel de experiencia")
df_limpio.groupBy("nivel_experiencia").avg("salario_usd").show()

# SQL: SELECT puesto, salario_usd FROM salarios ORDER BY salario_usd DESC LIMIT 10
# orderBy(): ordena, ascending=False es de mayor a menor. show(10) enseña solo 10 filas
print("\nCONSULTA 7: los 10 salarios mas altos")
df_limpio.select("puesto", "salario_usd").orderBy("salario_usd", ascending=False).show(10)

# SQL: SELECT DISTINCT puesto FROM salarios
# distinct(): quita los repetidos
# show(puestos.count(), truncate=False): como en examenes, muestra todas las filas y sin cortar el texto
print("\nCONSULTA 8: puestos distintos")
puestos = df_limpio.select("puesto").distinct()
puestos.show(puestos.count(), truncate=False)


# Cerrar la sesión de Spark al terminar
spark.stop()
