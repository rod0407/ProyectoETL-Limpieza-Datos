from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, initcap, when, count, upper, regexp_replace


# Inicializar Spark 

# Se crea un objeto llamado Spark, con el cual se trabajara en todo el progarma 
# SparkSession es el punto de entrada a Apache Spark
# builder significa que vas a configurar cómo quieres iniciar Spark
# cofig -> carga la libreria para leer archivos .xlsx 

#inicia la sesión de Apache Spark. Primero se crea la configuración de la sesión con SparkSession.builder, luego se asigna un nombre a la aplicación con appName, después se carga la librería spark-excel para poder leer archivos de Excel y, finalmente, getOrCreate() crea la sesión o reutiliza una que ya esté abierta

spark = SparkSession.builder \
    .appName("LimpiezaDatosGrupo") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.13:3.5.0_0.20.2") \
    .getOrCreate()


#ruta del archivo 
ruta_entrada = r"C:\Users\Sam Riv\Documents\SPARK_ENTREGA2\Datos_GRUPO.xlsx"
print(f"[INFO] Cargando archivo: {ruta_entrada}")



#carga el archivo de Excel en un DataFrame de spark, primero se indica que se utilizará la librería spark-excel para leer un archivo de Excel. Después, con header = true se establece que la primera fila contiene los nombres de las columnas. Con inferSchema = true, Spark identifica automáticamente el tipo de dato de cada columna, como texto, número o fecha. Finalmente, load(ruta_entrada) abre el archivo ubicado en la ruta especificada y guarda la información en el DataFrame df

df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(ruta_entrada)




print("\nESTRUCTURA ORIGINAL")
print(f"Total de filas: {df.count()}")
print("Nombre de columnas:", df.columns)

print("\n TIPOS DE DATOS DETECTADOS")
df.printSchema()


# Transformaciones 

print("LIMPIEZA DE DATOS")

#withColumn(): Crear una columna nueva, modificar una columna existente.

# withColumnRenamed(): cambia el nombre de una columna. En las siguientes líneas se usa df_limpio porque ya se está trabajando sobre el DataFrame que contiene los cambios anteriores

df_limpio = df.withColumnRenamed("Nombre", "NOMBRE")

df_limpio = df_limpio.withColumnRenamed("Direccion", "DIRECCION")

df_limpio = df_limpio.withColumnRenamed("Nombre de mascota", "NOMBRE MASCOTAS")

df_limpio = df_limpio.withColumnRenamed("Apodo de 2 mejores amigos", "APODOS DE AMIGOS")

df_limpio = df_limpio.withColumnRenamed("CURP", "CURP")

df_limpio = df_limpio.withColumnRenamed("Pasatiempo", "PASATIEMPO")

df_limpio = df_limpio.withColumnRenamed("Carrera", "CARRERA")

df_limpio = df_limpio.withColumnRenamed("Facultad", "FACULTAD")

df_limpio = df_limpio.withColumnRenamed("Cantante favorito", "CANTANTE FAVORITO")

df_limpio = df_limpio.withColumnRenamed("Edad", "EDAD")

df_limpio = df_limpio.withColumnRenamed("Banda Favorita", "BANDA FAVORITA")

df_limpio.show()


# trim(): elimina los espacios en blanco al inicio y al final del texto.
# col(): toma los valores que están dentro de la columna


df_limpio = df_limpio.withColumn("NOMBRE", trim(col("NOMBRE")))

df_limpio = df_limpio.withColumn("DIRECCION", trim(col("DIRECCION")))

df_limpio = df_limpio.withColumn("MASCOTAS", trim(col("MASCOTAS")))

df_limpio = df_limpio.withColumn("APODOS DE AMIGOS", trim(col("APODOS DE AMIGOS")))

df_limpio = df_limpio.withColumn("CURP", trim(col("CURP")))

df_limpio = df_limpio.withColumn("PASATIEMPO", trim(col("PASATIEMPO")))

df_limpio = df_limpio.withColumn("CARRERA", trim(col("CARRERA")))

df_limpio = df_limpio.withColumn("FACULTAD", trim(col("FACULTAD")))

df_limpio = df_limpio.withColumn("CANTANTE FAVORITO", trim(col("CANTANTE FAVORITO")))

df_limpio = df_limpio.withColumn("EDAD", trim(col("EDAD")))

df_limpio = df_limpio.withColumn("BANDA FAVORITA", trim(col("BANDA FAVORITA")))

df_limpio.show()

# 3Pasar todo a mayusculas 
# upper(): convierte texto a mayúsculas

df_limpio = df_limpio.withColumn("NOMBRE", upper(col("NOMBRE")))

df_limpio = df_limpio.withColumn("DIRECCION", upper(col("DIRECCION")))

df_limpio = df_limpio.withColumn("MASCOTAS", upper(col("MASCOTAS")))

df_limpio = df_limpio.withColumn("APODOS DE AMIGOS", upper(col("APODOS DE AMIGOS")))

df_limpio = df_limpio.withColumn("CURP", upper(col("CURP")))

df_limpio = df_limpio.withColumn("PASATIEMPO", upper(col("PASATIEMPO")))

df_limpio = df_limpio.withColumn("CARRERA", upper(col("CARRERA")))

df_limpio = df_limpio.withColumn("FACULTAD", upper(col("FACULTAD")))

df_limpio = df_limpio.withColumn("CANTANTE FAVORITO", upper(col("CANTANTE FAVORITO")))

df_limpio = df_limpio.withColumn("BANDA FAVORITA", upper(col("BANDA FAVORITA")))

df_limpio.show()


# Eliminar acentos, dieresis y ñ
# regexp_replace: sirve para buscar un carácter específico dentro de una columna y reemplazarlo por otro.
# regexp_replace(columna, texto_a_buscar, texto_nuevo)

    #NOMBRE
df_limpio = df_limpio.withColumn("NOMBRE", regexp_replace(col("NOMBRE"), "Á", "A"))
df_limpio = df_limpio.withColumn("NOMBRE", regexp_replace(col("NOMBRE"), "É", "E"))
df_limpio = df_limpio.withColumn("NOMBRE", regexp_replace(col("NOMBRE"), "Í", "I"))
df_limpio = df_limpio.withColumn("NOMBRE", regexp_replace(col("NOMBRE"), "Ó", "O"))
df_limpio = df_limpio.withColumn("NOMBRE", regexp_replace(col("NOMBRE"), "Ú", "U"))
df_limpio = df_limpio.withColumn("NOMBRE", regexp_replace(col("NOMBRE"), "Ü", "U"))
df_limpio = df_limpio.withColumn("NOMBRE", regexp_replace(col("NOMBRE"), "Ñ", "N"))

    #DIRECCION
df_limpio = df_limpio.withColumn("DIRECCION", regexp_replace(col("DIRECCION"), "Á", "A"))
df_limpio = df_limpio.withColumn("DIRECCION", regexp_replace(col("DIRECCION"), "É", "E"))
df_limpio = df_limpio.withColumn("DIRECCION", regexp_replace(col("DIRECCION"), "Í", "I"))
df_limpio = df_limpio.withColumn("DIRECCION", regexp_replace(col("DIRECCION"), "Ó", "O"))
df_limpio = df_limpio.withColumn("DIRECCION", regexp_replace(col("DIRECCION"), "Ú", "U"))
df_limpio = df_limpio.withColumn("DIRECCION", regexp_replace(col("DIRECCION"), "Ü", "U"))
df_limpio = df_limpio.withColumn("DIRECCION", regexp_replace(col("DIRECCION"), "Ñ", "N"))

    #MASCOTAS 
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Á", "A"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "É", "E"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Í", "I"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Ó", "O"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Ú", "U"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Ü", "U"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Ñ", "N"))

    #APODOS 
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Á", "A"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "É", "E"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Í", "I"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Ó", "O"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Ú", "U"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Ü", "U"))
df_limpio = df_limpio.withColumn("MASCOTAS", regexp_replace(col("MASCOTAS"), "Ñ", "N"))

    #PASATIEMPO 
df_limpio = df_limpio.withColumn("PASATIEMPO", regexp_replace(col("PASATIEMPO"), "Á", "A"))
df_limpio = df_limpio.withColumn("PASATIEMPO", regexp_replace(col("PASATIEMPO"), "É", "E"))
df_limpio = df_limpio.withColumn("PASATIEMPO", regexp_replace(col("PASATIEMPO"), "Í", "I"))
df_limpio = df_limpio.withColumn("PASATIEMPO", regexp_replace(col("PASATIEMPO"), "Ó", "O"))
df_limpio = df_limpio.withColumn("PASATIEMPO", regexp_replace(col("PASATIEMPO"), "Ú", "U"))
df_limpio = df_limpio.withColumn("PASATIEMPO", regexp_replace(col("PASATIEMPO"), "Ü", "U"))
df_limpio = df_limpio.withColumn("PASATIEMPO", regexp_replace(col("PASATIEMPO"), "Ñ", "N"))

    #CARRERA
df_limpio = df_limpio.withColumn("CARRERA", regexp_replace(col("CARRERA"), "Á", "A"))
df_limpio = df_limpio.withColumn("CARRERA", regexp_replace(col("CARRERA"), "É", "E"))
df_limpio = df_limpio.withColumn("CARRERA", regexp_replace(col("CARRERA"), "Í", "I"))
df_limpio = df_limpio.withColumn("CARRERA", regexp_replace(col("CARRERA"), "Ó", "O"))
df_limpio = df_limpio.withColumn("CARRERA", regexp_replace(col("CARRERA"), "Ú", "U"))
df_limpio = df_limpio.withColumn("CARRERA", regexp_replace(col("CARRERA"), "Ü", "U"))
df_limpio = df_limpio.withColumn("CARRERA", regexp_replace(col("CARRERA"), "Ñ", "N"))

    #FACULTAD 
df_limpio = df_limpio.withColumn("FACULTAD", regexp_replace(col("FACULTAD"), "Á", "A"))
df_limpio = df_limpio.withColumn("FACULTAD", regexp_replace(col("FACULTAD"), "É", "E"))
df_limpio = df_limpio.withColumn("FACULTAD", regexp_replace(col("FACULTAD"), "Í", "I"))
df_limpio = df_limpio.withColumn("FACULTAD", regexp_replace(col("FACULTAD"), "Ó", "O"))
df_limpio = df_limpio.withColumn("FACULTAD", regexp_replace(col("FACULTAD"), "Ú", "U"))
df_limpio = df_limpio.withColumn("FACULTAD", regexp_replace(col("FACULTAD"), "Ü", "U"))
df_limpio = df_limpio.withColumn("FACULTAD", regexp_replace(col("FACULTAD"), "Ñ", "N"))

    #CANTANTE FAVORITO 
df_limpio = df_limpio.withColumn("CANTANTE FAVORITO", regexp_replace(col("CANTANTE FAVORITO"), "Á", "A"))
df_limpio = df_limpio.withColumn("CANTANTE FAVORITO", regexp_replace(col("CANTANTE FAVORITO"), "É", "E"))
df_limpio = df_limpio.withColumn("CANTANTE FAVORITO", regexp_replace(col("CANTANTE FAVORITO"), "Í", "I"))
df_limpio = df_limpio.withColumn("CANTANTE FAVORITO", regexp_replace(col("CANTANTE FAVORITO"), "Ó", "O"))
df_limpio = df_limpio.withColumn("CANTANTE FAVORITO", regexp_replace(col("CANTANTE FAVORITO"), "Ú", "U"))
df_limpio = df_limpio.withColumn("CANTANTE FAVORITO", regexp_replace(col("CANTANTE FAVORITO"), "Ü", "U"))
df_limpio = df_limpio.withColumn("CANTANTE FAVORITO", regexp_replace(col("CANTANTE FAVORITO"), "Ñ", "N"))

    #BANDA FAVORITA 
df_limpio = df_limpio.withColumn("BANDA FAVORITA", regexp_replace(col("BANDA FAVORITA"), "Á", "A"))
df_limpio = df_limpio.withColumn("BANDA FAVORITA", regexp_replace(col("BANDA FAVORITA"), "É", "E"))
df_limpio = df_limpio.withColumn("BANDA FAVORITA", regexp_replace(col("BANDA FAVORITA"), "Í", "I"))
df_limpio = df_limpio.withColumn("BANDA FAVORITA", regexp_replace(col("BANDA FAVORITA"), "Ó", "O"))
df_limpio = df_limpio.withColumn("BANDA FAVORITA", regexp_replace(col("BANDA FAVORITA"), "Ú", "U"))
df_limpio = df_limpio.withColumn("BANDA FAVORITA", regexp_replace(col("BANDA FAVORITA"), "Ü", "U"))
df_limpio = df_limpio.withColumn("BANDA FAVORITA", regexp_replace(col("BANDA FAVORITA"), "Ñ", "N"))

df_limpio.show()

# Columna de Carrera 
# when(condición, valor_nuevo), 
# .otherwise(): si no cumple la condición deja el valor original.

df_limpio = df_limpio.withColumn(
    "CARRERA",
    when(col("CARRERA").contains("ING."), "INGENIERIA EN COMPUTACION")
    .when(col("CARRERA").contains("ING "), "INGENIERIA EN COMPUTACION")
    .otherwise(col("CARRERA"))
)

df_limpio.show()

# Forzar que la columna Edad sea un numero entero (integer)
# .cast("int"): Convierte los valores de esta columna a números enteros

df_limpio = df_limpio.withColumn("EDAD", col("EDAD").cast("int"))
df_limpio.show()

# Convertir NA, -, Ninguna a nulos reales
#.replace(): busca valores dentro de todo el DataFrame y los reemplaza.
df_limpio = df_limpio.replace(["NA", "-"], None)
df_limpio.show()

# Rellenar celdas vacias 
#fillna(): rellena valores nulo
df_limpio = df_limpio.fillna("NO ESPECIFICADO")
df_limpio.show()

#RESULTADO FINAL 

# truncate=False: evita que Spark corte textos largos

print("\n REGISTROS LIMPIOPS")
df_limpio.select(df_limpio.columns).show(df_limpio.count(), truncate=False)



print("\nESQUEMA FINAL")
df_limpio.printSchema()

#GUARDAR EL ARCHIVO DE SALIDA
ruta_salida = r"C:\Users\Sam Riv\Documents\SPARK_ENTREGA2\Datos_GRUPO_Limpio.xlsx"

#coalesce(1): sirve para que Spark genere un solo archivo de salida

df_limpio.coalesce(1).write \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(ruta_salida)

print("Archivo guardado correctamente")
# Cerrar la sesión de Spark al terminar
spark.stop()