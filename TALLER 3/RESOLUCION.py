# Databricks notebook source
# MAGIC %md
# MAGIC ## Parte 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 1

# COMMAND ----------

#1. Indique un reporte donde muestre el tiempo promedio de los viajes del 2018 por cada mes (en minutos). Starting_time – finish_time. 

# COMMAND ----------

storage_account_name = "grupo4taller3"

contenedor1 = "parte1"

storage_account_access_key = "sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2024-07-26T10:45:18Z&st=2024-07-12T02:45:18Z&spr=https&sig=Jf1hXQa0kp8q3sc%2BDF5wgQVv9Kkm1rzwWYIB4wPT2AI%3D"

spark.conf.set("fs.azure.sas."+contenedor1+"."+storage_account_name+".blob.core.windows.net",storage_account_access_key)

# COMMAND ----------

opciones = {"header": "true", "delimiter":",","inferSchema":"true"}

# COMMAND ----------

file_location = "wasbs://"+contenedor1+"@"+storage_account_name+".blob.core.windows.net"

df = spark.read.format("csv").options(**opciones).load(file_location + "/rides.txt")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql import SQLContext

sc = spark.sparkContext
#Inicializar la instancia de SQLContext 
sqlContext = SQLContext(sc)
sqlContext.registerDataFrameAsTable(df, "df")
parte_1_pregunta_1 = sqlContext.sql(''' select month(start_time) as MES_2018, ROUND(AVG(DATEDIFF(minute,start_time,finish_time)),0) AS PROMEDIO_VIAJE
                 from df
                 group by month(start_time) 
                 ORDER BY month(start_time) asc''')
parte_1_pregunta_1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 2

# COMMAND ----------

#2. Muestre un reporte indicando los 500 primeros servicios que han tenido mayor precio (Price), y su tiempo de uso.
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, TimestampType

# COMMAND ----------

df_2 = df.withColumn(
    "tiempo_uso",
    F.round( ((F.unix_timestamp(df['finish_time'], format='yyyy-MM-dd HH:mm:ss.SSS').cast('int') - 
     F.unix_timestamp(df['start_time'], format='yyyy-MM-dd HH:mm:ss.SSS').cast('int')) / 60),0
    )
)
parte_1_pregunta_2 = df_2.sort(F.col("price").desc(), F.col("tiempo_uso").desc()).limit(500)
parte_1_pregunta_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 3

# COMMAND ----------

#3. Agrupe un reporte por Scooter_id e indique los tiempos promedios en función de  estos, así como sus costos promedios. 
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

df_wduration = df \
    .withColumn('duration_min', F.round((F.col("finish_time").cast("long") - F.col("start_time").cast("long"))/60, 0)) \
    .withColumn('price', F.regexp_replace('price', ',', '.'))

df_wduration = df_wduration.withColumn('price', df_wduration["price"].cast(DoubleType()))

parte_1_pregunta_3 = df_wduration.groupBy('scooter_id').agg(F.round(F.avg('duration_min'), 2).alias('prom_tiempo_min'), F.round(F.avg('price'), 2).alias('prom_costo'))

display(parte_1_pregunta_3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 4

# COMMAND ----------

#4. ¿Quiénes son los usuarios que han usado más el servicio en el 2018?. Liste su ID y la cantidad de veces que han usado. 

import pyspark.sql.functions as F

df_alexis = df
#df_alexis.display()
df_alexis1 = df_alexis.withColumn('AñoUso', F.year(F.col('start_time')))
df_alexis1 = df_alexis1.filter(F.col('AñoUso') == '2018')
df_alexis1= df_alexis1.groupBy(F.col('user_id')).count()
df_alexis1 = df_alexis1.orderBy(F.col('count'),ascending = False)
parte_1_pregunta_4 = df_alexis1.select(F.col('user_id').alias('Usuario'),F.col('count').alias('CantUso'))
parte_1_pregunta_4.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 5

# COMMAND ----------

#5. Quienes son los 100 usuarios que han usado menos el servicio en el mismo año.
import pyspark.sql.functions as F
from pyspark.sql.window import Window

Pregunta5 = df_alexis.withColumn('AñoUso', F.year(F.col('start_time')))

Pregunta5 =Pregunta5.groupBy(F.col('user_id'),F.col('AñoUso')).count()
Pregunta5 = Pregunta5.select(F.col('user_id').alias('Usuario'),F.col('count').alias('CantUso'),F.col('AñoUso'))

Particion = Window.partitionBy(F.col('AñoUso')).orderBy(F.asc(F.col('CantUso')))

Pregunta5 = Pregunta5.withColumn('Orden',F.row_number().over(Particion))

parte_1_pregunta_5 = Pregunta5.filter(F.col('Orden')<= 100).drop(F.col('Orden'))

#Pregunta5.select(F.col('AñoUso')).distinct().display()
parte_1_pregunta_5.display()

# COMMAND ----------

# Cerramos conexión
spark.conf.unset('fs.azure.account.key'+storage_account_name+'.blob.core.windows.net')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 1

# COMMAND ----------

#1. Cree un blob storage con la estructura GRUPO_XXX_Taller3 donde XXX es el número del grupo 
# firma de acceso compartido (SAS) Storage grupo4taller3
storage_account_name = "grupo4taller3"

contenedor_parte_2 = "parte2"

#Extraemos firma de acceso compartido 
storage_account_access_key = "sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2024-07-21T03:20:14Z&st=2024-07-12T19:20:14Z&spr=https&sig=GZgYR03HvRlRVjh%2BofjrmT%2FAU9gyMdgUr54PN1E5Bjk%3D"

#Establecemos conexión entre la librería pyspark y el storage.
spark.conf.set('fs.azure.sas.'+contenedor_parte_2+"."+storage_account_name+'.blob.core.windows.net',storage_account_access_key)

dbutils.fs.ls('wasbs://'+contenedor_parte_2+"@"+storage_account_name+".blob.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 2

# COMMAND ----------

#Todos los reportes de la pregunta anterior, guárdenlo en el blob storage, utilice los 2 
#últimos métodos de autenticación vistos en clase.

# COMMAND ----------

# Parte 1 - Pregunta 1
fold = "parte_1_pregunta_1"
path = f"wasbs://parte2@"+storage_account_name+".blob.core.windows.net/"+fold
parte_1_pregunta_1.write.mode('overwrite').format('parquet').save(path)

# COMMAND ----------

# Parte 1 - Pregunta 2
fold = "parte_1_pregunta_2"
path = f"wasbs://parte2@"+storage_account_name+".blob.core.windows.net/"+fold
parte_1_pregunta_2.write.mode('overwrite').format('parquet').save(path)

# COMMAND ----------

# Parte 1 - Pregunta 3
fold = "parte_1_pregunta_3"
path = f"wasbs://parte2@"+storage_account_name+".blob.core.windows.net/"+fold
parte_1_pregunta_3.write.mode('overwrite').format('parquet').save(path)

# COMMAND ----------

# Cerramos conexión
spark.conf.unset('fs.azure.account.key'+storage_account_name+'.blob.core.windows.net')

# COMMAND ----------

# firma de acceso compartido (SAS) Contenedor Parte2
storage_account_name = "grupo4taller3"

contenedor_parte_2 = "parte2"

storage_account_access_key = "sp=racwdl&st=2024-07-12T20:24:10Z&se=2024-07-21T04:24:10Z&spr=https&sv=2022-11-02&sr=c&sig=HbkjCX%2Bkzni%2FVRd05lz3Tse1CeRefus5qeKU1FYaMwc%3D"

spark.conf.set('fs.azure.sas.'+contenedor_parte_2+"."+storage_account_name+'.blob.core.windows.net',storage_account_access_key)

dbutils.fs.ls('wasbs://'+contenedor_parte_2+"@"+storage_account_name+".blob.core.windows.net")

# COMMAND ----------

# Parte 1 - Pregunta 4
fold = "parte_1_pregunta_4"
path = f"wasbs://parte2@"+storage_account_name+".blob.core.windows.net/"+fold
parte_1_pregunta_4.write.mode('overwrite').format('parquet').save(path)

# COMMAND ----------

# Parte 1 - Pregunta 5
fold = "parte_1_pregunta_5"
path = f"wasbs://parte2@"+storage_account_name+".blob.core.windows.net/"+fold
parte_1_pregunta_5.write.mode('overwrite').format('parquet').save(path)

# COMMAND ----------

# Cerramos conexión
spark.conf.unset('fs.azure.account.key'+storage_account_name+'.blob.core.windows.net')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 3

# COMMAND ----------

#Cree un reporte que indique los usuarios que más usan el servicio (sobre el total de la 
# base) y deje esta información en el blob storage

# COMMAND ----------

contenedor_parte_2 = "parte2"

storage_account_access_key = "sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2024-07-21T03:20:14Z&st=2024-07-12T19:20:14Z&spr=https&sig=GZgYR03HvRlRVjh%2BofjrmT%2FAU9gyMdgUr54PN1E5Bjk%3D"

spark.conf.set('fs.azure.sas.'+contenedor_parte_2+"."+storage_account_name+'.blob.core.windows.net',storage_account_access_key)


# COMMAND ----------

storage_account_name = "grupo4taller3"
fold = "parte_2_pregunta_3"
path = f"wasbs://parte2@"+storage_account_name+".blob.core.windows.net/"+fold

parte_2_pregunta_3 =parte_1_pregunta_4
#UsoTotal = parte_2_pregunta_3.agg(F.sum(F.col('CantUso'))) #2684712
UsoTotal = parte_2_pregunta_3.agg(F.sum(F.col('CantUso')).alias('UsoTotal')).collect()[0]['UsoTotal']
#display(UsoTotal)
parte_2_pregunta_3 = parte_2_pregunta_3.withColumn('Distr%',F.round((F.col('CantUso')/UsoTotal)*100,4))
parte_2_pregunta_3.write.mode('overwrite').format('parquet').save(path)
parte_2_pregunta_3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 4

# COMMAND ----------

#4. Tomando como base la información del blob storage, cree un job que permita encontrar 
# al usuario que usó más el servicio en el 2018. Cree este proceso seccionado (no todo el 
# código en un mismo notebook) y finalmente guarde el resultado en el blob storage

# COMMAND ----------


