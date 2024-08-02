# Databricks notebook source
# MAGIC %md
# MAGIC ### Pregunta 1
# MAGIC
# MAGIC Construya una arquitectura basada en la necesidad del cliente. Al ser tiempo real,
# MAGIC proponga que componentes debiera tener el entorno usando databricks. El volumen
# MAGIC de data y frecuencia usted lo encuentra en los archivos de trabajo. Así mismo, debe
# MAGIC sincerar el costo que implica construirlo. Debe contar con una reportería BI planteada
# MAGIC en base a los inputs de información. 

# COMMAND ----------

#Se evalua cantidad de registros, frecuencia

# COMMAND ----------

# DBTITLE 1,Conexión
sas_token = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-07-29T13:18:14Z&st=2024-07-20T05:18:14Z&spr=https&sig=HJlSD076x5Lb2IepTHQQKRVzqoDkaoBVOp1tCTRqXOI%3D'
storage = 'grupo4taller4'
input_container = 'input'
output_container = 'output'

conexion = f"fs.azure.sas.{input_container}.{storage}.blob.core.windows.net"

# Configuración del acceso con SAS token
spark.conf.set(
    conexion,
    sas_token
)

input_container_ruta = f'wasbs://'+input_container+'@'+storage+'.blob.core.windows.net'

dbutils.fs.ls(input_container_ruta)


# COMMAND ----------

# DBTITLE 1,Lectura
opciones = {"header": "true", "delimiter":",","inferSchema":"true"}
open_app_df = spark.read.format("csv").options(**opciones).load(f"{input_container_ruta}/open_app.txt") 
rides_df = spark.read.format("csv").options(**opciones).load(f"{input_container_ruta}/rides.txt") 


# COMMAND ----------

import pyspark.sql.functions as Fuc

# COMMAND ----------

open_app_df.withColumn(
    "minute_time", Fuc.date_format(Fuc.col("open_time"), "yyyy-MM")
).groupBy("minute_time").agg(Fuc.count("*").alias("app_open_count")). \
    sort("minute_time",ascending = False).display()

# COMMAND ----------

open_app_df.withColumn(
    "minute_time", Fuc.date_format(Fuc.col("open_time"), "yyyy-MM-dd")
).groupBy("minute_time").agg(Fuc.count("*").alias("app_open_count")). \
    sort("minute_time",ascending = False).where(Fuc.col("minute_time") > "2018-07-31").display()

# COMMAND ----------

open_app_df.withColumn(
    "minute_time", Fuc.date_format(Fuc.col("open_time"), "yyyy-MM-dd HH:mm")
).groupBy("minute_time").agg(Fuc.count("*").alias("app_open_count")). \
    sort("minute_time",ascending = False).where(Fuc.col("minute_time") > "2018-09-10").display()

# COMMAND ----------

rides_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Según lo analizado, se puede concluir que existe un crecimiendo en gran volumen en los últimos dos meses. Con una tendencia a crecer y superar los 100,000 registros por día, así como la ingesta en tiempo real recomendada entre 100-200 registros por minuto. Razón por la cual se requiere un Servidor de Base de Datos con capacidad de procesamiento y memoria escalable. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 2
# MAGIC Genere un reporte usando spark sql, donde se muestre el tiempo promedio de
# MAGIC servicios (id) , agrupados de 100 en 100. Es decir, los 100 primeros , un tiempo
# MAGIC promedio, los 100 segundos, otro bloque, etc.

# COMMAND ----------

rides_df.display()

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# Se añade una columna ride_time para calcular minutos 
# Se añade una columna partition para colocar un ID incremental
# Se añade una columna id_new para hacer una división entera y calcular un ID incremental por grupos de 100 
rides_df_pregunta_2_v0 = rides_df \
    .withColumn(
        "ride_time",
        F.round( ((F.unix_timestamp(rides_df['finish_time'], format='yyyy-MM-dd HH:mm:ss.SSS').cast('int') - 
        F.unix_timestamp(rides_df['start_time'], format='yyyy-MM-dd HH:mm:ss.SSS').cast('int')) / 60),0
        )
    ) \
    .withColumn("partition", F.monotonically_increasing_id()) \
    .withColumn(
        "id_new", F.floor(F.col("partition") / 100) + 1
    )   
rides_df_pregunta_2_v0.display()

# COMMAND ----------

# Reporte agrupado por nuevo ID y promedio de viaje
rides_df_pregunta_2_agg = rides_df_pregunta_2_v0.groupBy("id_new").agg(
    F.avg(F.col("ride_time")).alias("avg_ride_time")
).sort("id_new", ascending = True)

rides_df_pregunta_2_agg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 3
# MAGIC El resultado déjelo en un parquet dentro de un blob storage en azure bajo el formato:
# MAGIC resultado_query1_grupo_xxx

# COMMAND ----------

# Conexión al contenedor "output"
sas_token = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-07-29T13:18:14Z&st=2024-07-20T05:18:14Z&spr=https&sig=HJlSD076x5Lb2IepTHQQKRVzqoDkaoBVOp1tCTRqXOI%3D'
storage = 'grupo4taller4'
output_container = 'output'

conexion = f"fs.azure.sas.{output_container}.{storage}.blob.core.windows.net"

# Configuración del acceso con SAS token
spark.conf.set(
    conexion,
    sas_token
)
path = f"wasbs://"+output_container+"@"+storage+".blob.core.windows.net"
dbutils.fs.ls(path)

# COMMAND ----------

# parquet file: resultado_query1_grupo_4
fold = "resultado_query1_grupo_4"
path += f"/"+fold

rides_df_pregunta_2_agg.write.mode('overwrite').format('parquet').save(path)
dbutils.fs.ls(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 4
# MAGIC Genere un reporte donde se indique los 100 servicios que han costado más, y los 100
# MAGIC que costaron menos. Debe estar consolidado en un solo reporte. Dejelo en un blob
# MAGIC storage bajo la estructura resultado_query2_grupo_xxx

# COMMAND ----------

#Se verifica el schema para ver la columna price
from pyspark.sql.types import DoubleType
rides_df.printSchema()

# COMMAND ----------

#Se castea a Double la columna price para que el ordenamiento sea el adecuado
rides_df_clean = rides_df.withColumn(
    "price", F.round((F.regexp_replace('price', ',', '.')).cast(DoubleType()) ,2)
)

rides_df_pregunta_4 = \
    rides_df_clean.sort("price", ascending = False).limit(100).union(
        rides_df_clean.sort("price", ascending = True).limit(100)
    )

rides_df_pregunta_4.display()

# COMMAND ----------

# Se registra en un parquet el reporte del top 100
# Conexión al contenedor "output"
sas_token = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-07-29T13:18:14Z&st=2024-07-20T05:18:14Z&spr=https&sig=HJlSD076x5Lb2IepTHQQKRVzqoDkaoBVOp1tCTRqXOI%3D'
storage = 'grupo4taller4'
output_container = 'output'

conexion = f"fs.azure.sas.{output_container}.{storage}.blob.core.windows.net"

# Configuración del acceso con SAS token
spark.conf.set(
    conexion,
    sas_token
)
path = f"wasbs://"+output_container+"@"+storage+".blob.core.windows.net"
dbutils.fs.ls(path)


# COMMAND ----------

# parquet file: resultado_query2_grupo_4
fold = "resultado_query2_grupo_4"
path += f"/"+fold

rides_df_pregunta_4.write.mode('overwrite').format('parquet').save(path)
dbutils.fs.ls(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 5
# MAGIC Para generar las proyecciones en tiempo real, le dan la siguiente lógica:
# MAGIC
# MAGIC a. Se van a generar 500 ID’s adicionales.
# MAGIC
# MAGIC b. El start_time va a tener la fecha entre cualquier día/mes del año 2020.
# MAGIC
# MAGIC c. El finish_time va a ser un aleatorio entre 0-45 minutos tomando como base el startime.
# MAGIC
# MAGIC d. El precio va a ser una función definida bajo la siguiente lógica:
# MAGIC
# MAGIC     i. Si la diferencia entre el starttime y el finishtime es menor a 1 minuto, el precio es un aleatorio entre 1-5 multiplicado * 2.5
# MAGIC
# MAGIC     ii. Si la diferencia entre el starttime y el finishtime es entre 1 y 5 minutos, el precio es un aleatorio entre entre 5-10 multiplicado *3
# MAGIC
# MAGIC     iii. Para todo lo demás, el precio es un aleatorio tomando el precio mayor disponible en la BD multiplicado * 0.9

# COMMAND ----------

#Vamos a crear una BBDD para luego integrarlo a nuestro delta lake

db = "deltaEspGrupo4Taller4"

#Crear la base de datos

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

spark.sql(f"USE {db}")

#Preparar nuestra base de datos y configurar para habilitar DL
spark.sql(" SET spark.databricks.delta.formatCheck.enable = false ")
spark.sql(" SET spark.databricks.delta.properties.autoOptimize.optimizeWrite = true ")

# COMMAND ----------

import random
from datetime import datetime
from datetime import timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

rides_df = rides_df.withColumn('price', regexp_replace('price', ',', '.')) \
                    .withColumn('price', col("price").cast(DoubleType()))

# COMMAND ----------

def my_checkpoint():
    return 'tmp/deltaEspGrupo4Taller4/chkpt/%s'%random.randint(0,10000)

# Generamos una función para retornar data de manera aleatoria 
@udf(returnType=StringType())
def random_start_time():
    inicio = datetime(2020, 1, 1)
    final =  datetime(2020, 12, 31)
    random_start = inicio + (final - inicio) * random.random()
    return random_start.timestamp()

# COMMAND ----------

# Crear una función donde de forma aleatoria va a empezar a cargar data
def genera_data_stream(table_format, table_name, schema_ok = False):

    streaming_data = (
        spark.readStream.format("rate").option("rowsPerSecond", 10).load()
        .withColumn("id", round(10000000*rand(), 0))
        .withColumn("start_time", random_start_time())
        .withColumn('start_time', col("start_time").cast("double").cast("timestamp"))
        .withColumn("duration", round(45*60*rand(), 0))
        .withColumn("finish_time", 
            (unix_timestamp(col("start_time").cast("timestamp")) + col("duration")).cast('timestamp'))
        .withColumn("price", when(col("duration")/60 < 1, (1 + 4*rand())*2.5).when((col("duration")/60 >= 1) & (col("duration")/60 <= 5), (5 + 5*rand())*3).otherwise(lit(rides_df.agg(max("price")).collect()[0][0]*2)))
        .withColumn("price", round(col("price"), 2))
    )

    if schema_ok:
        streaming_data = streaming_data.select("id", "start_time", "finish_time", "price", "timestamp")
    
    query =(
        streaming_data.writeStream
        .format(table_format)
        .option("checkpointLocation", my_checkpoint())
        .trigger(processingTime="5 seconds")
        .table(table_name)
    )

    return query

# COMMAND ----------

def stop_all_stream():
    print('Parando todos los streams DMC Databricks')

    for s in spark.streams.active:
        try:
            s.stop()
        except:
            pass

    print('Todos los streams fueron detenidos')

# Crear una función para eliminar los path creados para almacenar la data y las tablas delta
def limpiar_path_tablas():
    dbutils.fs.rm('tmp/deltaEspGrupo4Taller4/chkpt', True)
    dbutils.fs.rm('file:/dbfs/tmp/deltaEspGrupo4Taller4/loans_parquet/',True)

    for table in ["deltaEspGrupo4Taller4.grupo4pregunta5"]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")

# COMMAND ----------

stop_all_stream()

# COMMAND ----------

stream_query = genera_data_stream(table_format="delta", table_name="grupo4pregunta5", schema_ok=True)

# COMMAND ----------

df_grupo4pregunta5 = spark.sql("select * from grupo4pregunta5")
display(df_grupo4pregunta5)

# COMMAND ----------

limpiar_path_tablas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 6

# COMMAND ----------

# MAGIC %md
# MAGIC Generamos una foto de la misma dataframe de 500 en la ruta
# MAGIC

# COMMAND ----------

df_grupo4pregunta5.write.mode('overwrite').parquet('file:/dbfs/tmp/deltaEspGrupo4Taller4/Dataframe500')
