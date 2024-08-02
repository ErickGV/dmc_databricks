# Databricks notebook source
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

sqlContext = SQLContext(spark.sparkContext)
spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

df_view = spark.sql("SELECT * FROM global_temp.users_web")

# COMMAND ----------

df_view = df_view.withColumn('Year', F.year('open_time'))
df_grp_us = df_view.filter(df_view['Year'] == 2018)
df_grp_us = df_grp_us.groupBy('user_id').agg(F.count(F.col('open_time')).alias('num_visit')).orderBy('num_visit', ascending=False)
df_user_top1 = df_grp_us.first()

# COMMAND ----------

schema = StructType(
    [StructField("user_id", IntegerType(), True),
    StructField("num_visit", IntegerType(), True)]
)
parte_2_pregunta_4 = spark.createDataFrame([df_user_top1], schema=schema)
display(parte_2_pregunta_4)

# COMMAND ----------

contenedor_parte_2 = "parte2"
storage_account_name = "grupo4taller3"
storage_account_access_key = "sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2024-07-21T03:20:14Z&st=2024-07-12T19:20:14Z&spr=https&sig=GZgYR03HvRlRVjh%2BofjrmT%2FAU9gyMdgUr54PN1E5Bjk%3D"
spark.conf.set('fs.azure.sas.'+contenedor_parte_2+"."+storage_account_name+'.blob.core.windows.net',storage_account_access_key)


# COMMAND ----------

fold = "parte_2_pregunta_4"
storage_account_name = "grupo4taller3"
path = f"wasbs://parte2@"+storage_account_name+".blob.core.windows.net/"+fold
parte_2_pregunta_4.write.mode('append').format('parquet').save(path)
