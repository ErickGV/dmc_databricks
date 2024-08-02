# Databricks notebook source
df_ventas = spark.read.table("ventas_ejercicio_erick_gonzales")

# COMMAND ----------

from pyspark.sql.functions import col, lit, round

# COMMAND ----------

df_ventas = df_ventas.withColumn("precio_total", round(df_ventas["CANTIDAD"] * df_ventas["PRECIO"],4))
df_ventas.display()

# COMMAND ----------

df_ventas = df_ventas.withColumn("impuesto", round(18 / 100 * df_ventas["precio_total"],4))
df_ventas.display()

# COMMAND ----------

# Datos y nombres de columnas
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
columns = ["language", "users_count"]

# Crear el DataFrame
df = spark.createDataFrame(data, columns)

# Mostrar el esquema del DataFrame
df.printSchema()

# COMMAND ----------

# Crear un RDD a partir de una lista
rdd = spark.sparkContext.parallelize(data)

# Crear el DataFrame desde el RDD
df_from_rdd = rdd.toDF(*columns)

# Mostrar el esquema del DataFrame
df_from_rdd.printSchema()

# COMMAND ----------

rdd.collect()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType


# COMMAND ----------

datos_lenguajes = [("Java", 20000), ("Python", 100000), ("Scala", 3000), ("PHP", 50000)]
columnas = ["language", "users_count"]
schema = StructType([
    StructField("language", StringType(), True),
    StructField("users_count", IntegerType(), True),
])

# COMMAND ----------

df_datos_lenguajes = spark.createDataFrame(data = datos_lenguajes,schema = schema)
df_datos_lenguajes.display()

# COMMAND ----------

#dbfs:/FileStore/data/VENTAS_EJERCICIO.txt
#from pyspark.sql.types import *
path_file = "dbfs:/FileStore/data/VENTAS_EJERCICIO.txt"
schema_venta = StructType([
    StructField("CODIGO", IntegerType(), True),
    StructField("PRODUCTO", StringType(), True),
    StructField("CLIENTE", StringType(), True),
    StructField("CANTIDAD", FloatType(), True),
    StructField("PRECIO", FloatType(), True),
])
df_ventas_file = spark.read.format("csv").option("header", "True").option("delimiter", ",").schema(schema_venta).load(path_file)

# COMMAND ----------

df_ventas_file.select('CLIENTE','CANTIDAD').display()

# COMMAND ----------

#df_ventas_file.schema
#df_ventas_file.withColumn("PRECIO", col("PRECIO").cast("String")).schema
df_ventas_file.withColumn("PRECIO_2", col("PRECIO") * 2).display()
#df_ventas_file.count()
#df_ventas_file.dropDuplicates()
#df_ventas_file.drop("PRECIO_2")
#df_ventas_file.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_ventas_file.groupBy("PRODUCTO","CLIENTE").agg( 
    sum("CANTIDAD").alias("CANTIDAD_TOTAL"),
    sum("PRECIO").alias("VENTA_TOTAL"),    
).sort("PRODUCTO", ascending = False).display()
