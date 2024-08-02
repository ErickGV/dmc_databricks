# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

# COMMAND ----------

schema_estudiantes = StructType([
    StructField("ID_DUPLICATE", IntegerType(), True),
    StructField("ID", IntegerType(), True),
    StructField("NIVEL_EDUCATIVO", StringType(), True),
    StructField("SEXO", StringType(), True),
    StructField("CATEGORIA_EMPLEO", StringType(), True),    
    StructField("EXPERIENCIA_LABORAL", StringType(), True),
    StructField("ESTADO_CIVIL", StringType(), True),
    StructField("EDAD", IntegerType(), True),
    StructField("UTILIZACION_TARJETAS", IntegerType(), True),
    StructField("NUMERO_ENTIDADES", StringType(), True),
    StructField("DEFAULT", IntegerType(), True),
    StructField("NUM_ENT_W", FloatType(), True),
    StructField("EDUC_W", FloatType(), True),
    StructField("EXP_LAB_W", FloatType(), True),
    StructField("EDAD_W", FloatType(), True),
    StructField("UTIL_TC_W", FloatType(), True),
    StructField("PD", FloatType(), True),
    StructField("RPD", FloatType(), True),
])

path_file = f"dbfs:/FileStore/data/DATA_EJERCICIO_S4_ERICK_GONZALES.csv"
df_estudiantes = spark.read.format("csv").option("header", "True").option("delimiter", ";").schema(schema_estudiantes).load(path_file)

# COMMAND ----------

df_estudiantes = df_estudiantes.drop('ID_DUPLICATE')

# COMMAND ----------

df_estudiantes.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_estudiantes = df_estudiantes.withColumn("EDAD_MAXIMA_ENDEUDAMIENTO", round(1.05 * df_estudiantes["EDAD"],1))
df_estudiantes = df_estudiantes.withColumn("PROBABILIDAD_MAXIMA_DEFAULT", round(1.08 * df_estudiantes["RPD"],2))
df_estudiantes.display()

# COMMAND ----------

from pyspark.sql import SQLContext

sc = spark.sparkContext

# COMMAND ----------

#Inicializar la instancia de SQLContext 

sqlContext = SQLContext(sc)

# COMMAND ----------

sqlContext.registerDataFrameAsTable(df_estudiantes, "table_estudiantes")

# COMMAND ----------

sqlContext.sql(" select ID, SEXO from table_estudiantes ").display()

# COMMAND ----------

# MAGIC %md
# MAGIC EJERCICIO
# MAGIC
# MAGIC Elaborar una consulta donde se muestren los clientes mayores a 35 años con menores probabilidades de default (los 5 primeros).
# MAGIC
# MAGIC Elaborar una consulta donde se muestren los solteros menores a 35 años que utilizan más de 3 veces su tc y tienen menor probabilidad de default (10 primeros)

# COMMAND ----------

sqlContext.sql(""" 
            select * 
            from table_estudiantes 
            where EDAD > 35  
            order by PD asc
            limit 5
               """).display()

# COMMAND ----------

sqlContext.sql(""" 
            select * 
            from table_estudiantes 
            where 
                EDAD < 35  
                AND ESTADO_CIVIL = 'SOLTERO'
                AND UTILIZACION_TARJETAS > 3
            order by PD asc
            limit 10
               """).display()

# COMMAND ----------

df_estudiantes.groupBy("SEXO").agg( 
    mean("EDAD").alias("EDAD_PROMEDIO"),    
).sort("SEXO", ascending = False).limit(1).display()
