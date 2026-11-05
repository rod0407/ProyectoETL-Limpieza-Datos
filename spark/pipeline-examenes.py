from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when, regexp_replace


# Inicializar Spark

# SparkConf: la configuracion de la aplicacion, aqui le ponemos nombre con setAppName y el numero de particiones con set
# SparkContext: la conexion con Spark, se crea con la configuracion
# SparkSession: se crea con el SparkContext y es la que usamos para leer archivos y trabajar con DataFrames

# spark.sql.shuffle.partitions: cuando Spark agrupa o quita repetidos reparte los datos en 200 partes por defecto
# como son pocos datos con 4 partes basta, y con 200 a veces Spark 2 fallaba en compus con muchos nucleos
conf = SparkConf().setAppName("LimpiezaDatosGrupo").set("spark.sql.shuffle.partitions", "4")
sc = SparkContext(conf=conf)
# se agrego porque los mensajes INFO de Spark no dejaban ver la salida de las consultas, asi solo muestra advertencias y errores
sc.setLogLevel("WARN")
spark = SparkSession(sc)


# ruta del archivo, el programa se ejecuta desde la carpeta spark/ por eso se usa ".." para subir a la carpeta del proyecto y luego entrar a datos/
ruta_entrada = "../datos/datos-grupo.csv"
print(f"[INFO] Cargando archivo: {ruta_entrada}")



# Carga el archivo CSV en un DataFrame de Spark.
# format("csv"): indica que el archivo es un CSV.
# header = true: la primera fila contiene los nombres de las columnas.
# inferSchema = true: Spark identifica automáticamente el tipo de dato de cada columna, como texto o número.
# load(ruta_entrada): abre el archivo y guarda la información en el DataFrame df

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(ruta_entrada)




# count(): cuenta cuántas filas tiene el DataFrame
# columns: es la lista con los nombres de las columnas
print("\nESTRUCTURA ORIGINAL")
print(f"Total de filas: {df.count()}")
print("Nombre de columnas:", df.columns)

# printSchema(): muestra cada columna con su tipo de dato (string = texto, integer = número entero)
print("\n TIPOS DE DATOS DETECTADOS")
df.printSchema()


# Transformaciones 

print("LIMPIEZA DE DATOS")

#withColumn(): Crear una columna nueva, modificar una columna existente.

# withColumnRenamed(): cambia el nombre de una columna. En las siguientes líneas se usa df_limpio porque ya se está trabajando sobre el DataFrame que contiene los cambios anteriores
# ojo: en el CSV la columna se llama "Nombre de Mascota" con M mayuscula, pero a Spark no le importan las mayusculas en los nombres de columnas y por eso si la encuentra
# show(): muestra en pantalla las primeras 20 filas del DataFrame

df_limpio = df.withColumnRenamed("Nombre", "NOMBRE")

df_limpio = df_limpio.withColumnRenamed("Direccion", "DIRECCION")

df_limpio = df_limpio.withColumnRenamed("Nombre de mascota", "MASCOTAS")

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
# trim tambien convierte EDAD a texto, por eso mas abajo se regresa a numero con cast("int")


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

# Pasar todo a mayúsculas
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
# regexp viene de expresion regular, pero aqui solo lo usamos para buscar una letra
# como el texto ya esta en mayusculas solo buscamos las letras con acento en mayuscula

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
df_limpio = df_limpio.withColumn("APODOS DE AMIGOS", regexp_replace(col("APODOS DE AMIGOS"), "Á", "A"))
df_limpio = df_limpio.withColumn("APODOS DE AMIGOS", regexp_replace(col("APODOS DE AMIGOS"), "É", "E"))
df_limpio = df_limpio.withColumn("APODOS DE AMIGOS", regexp_replace(col("APODOS DE AMIGOS"), "Í", "I"))
df_limpio = df_limpio.withColumn("APODOS DE AMIGOS", regexp_replace(col("APODOS DE AMIGOS"), "Ó", "O"))
df_limpio = df_limpio.withColumn("APODOS DE AMIGOS", regexp_replace(col("APODOS DE AMIGOS"), "Ú", "U"))
df_limpio = df_limpio.withColumn("APODOS DE AMIGOS", regexp_replace(col("APODOS DE AMIGOS"), "Ü", "U"))
df_limpio = df_limpio.withColumn("APODOS DE AMIGOS", regexp_replace(col("APODOS DE AMIGOS"), "Ñ", "N"))

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
# contains(): revisa si el texto incluye "ING." o "ING ", si es asi toda la carrera se cambia a INGENIERIA EN COMPUTACION

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

# Convertir NA y - a nulos reales
#.replace(): busca valores dentro de todo el DataFrame y los reemplaza.
# solo cambia cuando todo el valor es "NA" o "-" y solo en columnas de texto, None quiere decir nulo
df_limpio = df_limpio.replace(["NA", "-"], None)
df_limpio.show()

# Rellenar celdas vacias
#fillna(): rellena valores nulo
# como el relleno es texto solo cambia las columnas de texto, EDAD se queda igual
df_limpio = df_limpio.fillna("NO ESPECIFICADO")
df_limpio.show()

#RESULTADO FINAL

# select(df_limpio.columns): elige todas las columnas del DataFrame
# show(df_limpio.count()): muestra todas las filas
# truncate=False: evita que Spark corte textos largos

print("\n REGISTROS LIMPIOPS")
df_limpio.select(df_limpio.columns).show(df_limpio.count(), truncate=False)



print("\nESQUEMA FINAL")
df_limpio.printSchema()

#GUARDAR EL ARCHIVO DE SALIDA
# Spark guarda una carpeta con un archivo part-00000-....csv adentro
ruta_salida = "datos-salida/examenes_limpios"

#coalesce(1): junta todos los datos en una sola parte, para que dentro de la carpeta quede un solo archivo .csv
# header = true: escribe los nombres de las columnas en la primera fila
# mode("overwrite"): si la carpeta ya existe, la reemplaza

df_limpio.coalesce(1).write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(ruta_salida)

print("Archivo guardado correctamente")


# CONSULTAS
# lo mismo que haríamos con SQL pero con funciones de DataFrame, arriba de cada una esta la consulta en SQL

# SQL: SELECT * FROM examenes
# show() muestra hasta 20 filas, como son 15 salen todas
print("\nCONSULTA 1: toda la tabla")
df_limpio.show()

# SQL: SELECT * FROM examenes LIMIT 5
# show(5): muestra solo las primeras 5 filas
print("\nCONSULTA 2: primeras 5 filas")
df_limpio.show(5)

# SQL: SELECT NOMBRE, CARRERA, EDAD FROM examenes
# select(): elige solo las columnas indicadas
print("\nCONSULTA 3: solo algunas columnas")
df_limpio.select("NOMBRE", "CARRERA", "EDAD").show()

# SQL: SELECT COUNT(*) FROM examenes
# count(): cuenta cuántas filas hay
print("\nCONSULTA 4: total de registros")
print(df_limpio.count())

# SQL: SELECT NOMBRE, EDAD FROM examenes WHERE EDAD > 22
# where(): deja solo las filas que cumplen la condición
print("\nCONSULTA 5: personas mayores de 22 años")
df_limpio.select("NOMBRE", "EDAD").where(col("EDAD") > 22).show()

# SQL: SELECT CARRERA, COUNT(*) FROM examenes GROUP BY CARRERA
# groupBy(): junta las filas con el mismo valor, count(): cuenta cuantas hay en cada grupo
print("\nCONSULTA 6: cantidad de personas por carrera")
df_limpio.groupBy("CARRERA").count().show()

# SQL: SELECT NOMBRE, EDAD FROM examenes ORDER BY EDAD DESC
# orderBy(): ordena, ascending=False es de mayor a menor
print("\nCONSULTA 7: ordenados por edad, de mayor a menor")
df_limpio.select("NOMBRE", "EDAD").orderBy("EDAD", ascending=False).show()

# SQL: SELECT DISTINCT FACULTAD FROM examenes
# distinct(): quita los valores repetidos
print("\nCONSULTA 8: facultades distintas")
df_limpio.select("FACULTAD").distinct().show()


# Cerrar la sesión de Spark al terminar
spark.stop()