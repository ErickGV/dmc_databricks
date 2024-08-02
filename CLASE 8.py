# Databricks notebook source
#Vamos a crear una BBDD para luego integrarlo a nuestro delta lake

db = "deltaesperickgv"

#Crear la base de datos

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

spark.sql(f"USE {db}")

#Preparar nuestra base de datos y configurar para habilitar DL
spark.sql(" SET spark.databricks.delta.formatCheck.enable = false ")
spark.sql(" SET spark.databricks.delta.properties.autoOptimize.optimizeWrite = true ")

# COMMAND ----------

#Vamos a importar algunas librerías necesarias

import random
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *


#Creamos nuestra función para devolver la ruta del checkpoint
def my_checkpoint():
    return 'tmp/deltaesperickgv/chkpt/%s'%random.randint(0,10000)

#Generamos una función para retornar data de manera aleatoria 
@udf(returnType=StringType())
def random_provincias():
    return random.choice(["CA","TX","NY","WA"])

# COMMAND ----------

#Vamos a importar algunas librerías necesarias

import random
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *


#Creamos nuestra función para devolver la ruta del checkpoint
def my_checkpoint():
    return 'tmp/deltaesperickgv/chkpt/%s'%random.randint(0,10000)

#Generamos una función para retornar data de manera aleatoria 
@udf(returnType=StringType())
def random_provincias():
    return random.choice(["CA","TX","NY","WA"])

#Crear una función donde de forma aleatoria va a empezar a cargar data y asociar a tablas parquet
def genera_data_stream(table_format, table_name, schema_ok = False, type = "batch"):

    streaming_data = (
        spark.readStream.format("rate").option("rowsPerSecond", 100).load()
        .withColumn("loan_id", 1000 + col("value"))
        .withColumn("funded_amnt", (rand() * 3000 + 2000).cast("integer"))
        .withColumn("paid_amnt", col("funded_amnt") - rand() * 200)
        .withColumn("addr_state", random_provincias())
        .withColumn("type", lit(type))
    )

    if schema_ok:
        streaming_data = streaming_data.select("loan_id","funded_amnt","paid_amnt","addr_state","type","timestamp")
    
    query =(
        streaming_data.writeStream
        .format(table_format)
        .option("checkpointLocation", my_checkpoint())
        .trigger(processingTime="5 seconds")
        .table(table_name)
    )

    return query





# COMMAND ----------

# Crear funciones para detener los procesos streamin, y así no se queden ejecutando innecesariamente

def stop_all_stream():
    print('Parando todos los streams DMC Databricks')

    for s in spark.streams.active:
        try:
            s.stop()
        except:
            pass

    print('Todos los streams fueron detenidos')

#Crear una función para eliminar los path creados para almacenar la data y las tablas delta
def limpiar_path_tablas():
    dbutils.fs.rm('tmp/deltaesperickgv/chkpt', True)
    dbutils.fs.rm('file:/dbfs/tmp/deltaesperickgv/loans_parquet/',True)

for table in ["deltaesperickgv.loans_parquet", "deltaesperickgv.loans_delta","deltaesperickgv.loans_delta2"]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


# COMMAND ----------

# MAGIC
# MAGIC %sh mkdir -p /dbfs/tmp/deltaesperickgv/loans_parquet/; wget -O /dbfs/tmp/deltaesperickgv/loans_parquet/loans.parquet https://pages.databricks.com/rs/094-YMS-629/images/SAISEU19-loan-risks.snappy.parquet

# COMMAND ----------

dbutils.fs.ls('tmp/deltaesperickgv/loans_parquet')

# COMMAND ----------

parquet_path = "file:/dbfs/tmp/deltaesperickgv/loans_parquet"

df = (
    spark.read.format("parquet").load(parquet_path)
    .withColumn("type", lit("batch"))
    .withColumn("timestamp", current_timestamp())
)

df.write.format("delta").mode("overwrite").saveAsTable("loans_delta")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE loans_delta2
# MAGIC USING delta
# MAGIC as SELECT * FROM PARQUET.`/tmp/deltaesperickgv/loans_parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM loans_delta2 LIMIT 100

# COMMAND ----------

stream_query_A = genera_data_stream(table_format="delta", table_name="loans_delta", schema_ok=True, type="stream A")

stream_query_B = genera_data_stream(table_format="delta", table_name="loans_delta", schema_ok=True, type="stream B")

# COMMAND ----------

display(spark.readStream.format("delta").table("loans_delta").groupBy("type").count())

# COMMAND ----------

spark.sql(" select * from loans_delta where type = 'stream A' ").show()

# COMMAND ----------

stop_all_stream()
limpiar_path_tablas()
