# Databricks notebook source
# MAGIC %md
# MAGIC PREGUNTA 1
# MAGIC
# MAGIC Cree un proceso, donde una vez que la data se deje en una ruta, este pueda cargarse
# MAGIC de forma directa, con los tipos de datos que corresponden a cada variable.

# COMMAND ----------

from pyspark.sql.types import *

ruta_file = 'dbfs:/FileStore/data/G4/DATA_TALLER_2_.csv'

schema = StructType([
    StructField("ID", IntegerType(), True), \
    StructField("ID_1", IntegerType(), True), \
    StructField("NIVEL_EDUCATIVO", StringType(), True), \
    StructField("SEXO", StringType(), True), \
    StructField("CATEGORIA_EMPLEO", StringType(), True), \
    StructField("EXPERIENCIA_LABORAL", IntegerType(), True), \
    StructField("ESTADO_CIVIL", StringType(), True), \
    StructField("EDAD", IntegerType(), True), \
    StructField("UTILIZACION_TARJETAS", IntegerType(), True), \
    StructField("NUMERO_ENTIDADES", StringType(), True), \
    StructField("DEFAULT", IntegerType(), True), \
    StructField("NUM_ENT_W", FloatType(), True), \
    StructField("EDUC_W", FloatType(), True), \
    StructField("EXP_LAB_W", FloatType(), True), \
    StructField("EDAD_W", FloatType(), True), \
    StructField("UTIL_TC_W", FloatType(), True), \
    StructField("PD", FloatType(), True), \
    StructField("RPD", FloatType(), True), \
])

df_clientes_original = spark.read.option("header",True).schema(schema).csv(ruta_file, sep = ";")

# COMMAND ----------

# MAGIC %md
# MAGIC PREGUNTA 2
# MAGIC
# MAGIC Elimine todas las variables que contienen peso (terminan en _W) y liste el dataframe
# MAGIC depurado

# COMMAND ----------

df_clientes_depurado = df_clientes_original.drop("NUM_ENT_W").drop("EDUC_W").drop("EXP_LAB_W").drop("EDAD_W").drop("UTIL_TC_W").drop("ID_1")
df_clientes_depurado.display()

# COMMAND ----------

# MAGIC %md
# MAGIC PREGUNTA 3
# MAGIC
# MAGIC Liste un reporte donde muestre solo las personas con nivel educativo “Estudios
# MAGIC Universitarios” con una “PD” más alta.

# COMMAND ----------

df_clientes_estudiante_universitario = df_clientes_depurado.filter(df_clientes_depurado['NIVEL_EDUCATIVO'] == 'Estudios Universitarios').orderBy('PD', ascending = 0)
display(df_clientes_estudiante_universitario)

# COMMAND ----------

display(df_clientes_depurado.filter(df_clientes_depurado['NIVEL_EDUCATIVO'] == 'Estudios Universitarios').orderBy('PD', ascending=0))

# COMMAND ----------

# MAGIC %md
# MAGIC PREGUNTA 4
# MAGIC
# MAGIC Tomando la pregunta 3 de referencia, indique solo los primeros 5

# COMMAND ----------

df_clientes_estudiante_universitario.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC PREGUNTA 5
# MAGIC
# MAGIC Genere un cuadro consolidado donde indique la cantidad de personas que tienen
# MAGIC estudios técnicos y estudios universitarios

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_clientes_depurado.groupBy("NIVEL_EDUCATIVO").agg(
    count_distinct('ID').alias('CANTIDAD_PERSONAS')
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC PREGUNTA 6
# MAGIC
# MAGIC De la pregunta anterior, cree un dataframe solo con los de estudios técnicos y sus PD.
# MAGIC Listelas de mayor a menor por PD. Seleccione un nuevo dataframe donde solo muestre
# MAGIC los 5 registros con mejores PD y los 5 peores.

# COMMAND ----------

df_estudos_tecnicos = df_clientes_depurado.select("NIVEL_EDUCATIVO","PD").filter(df_clientes_depurado["NIVEL_EDUCATIVO"] == 'Estudios Tecnicos').sort("PD", ascending = False)
df_estudos_tecnicos.display()

# COMMAND ----------

df_estudos_tecnicos_mejores_5 = df_estudos_tecnicos.sort("PD", ascending = False).limit(5)
df_estudos_tecnicos_peores_5 = df_estudos_tecnicos.sort("PD", ascending = True).limit(5)
df_estudos_tecnicos_combinados = df_estudos_tecnicos_mejores_5.union(
    df_estudos_tecnicos_peores_5
)
df_estudos_tecnicos_combinados.display()

# COMMAND ----------

# MAGIC %md
# MAGIC PREGUNTA 7

# COMMAND ----------

# MAGIC %md
# MAGIC Similar a la pregunta 6, realice el mismo caso para los que tienen estudios
# MAGIC universitarios
# MAGIC

# COMMAND ----------

df_estudos_universitarios = df_clientes_depurado.select("NIVEL_EDUCATIVO","PD").filter(df_clientes_depurado["NIVEL_EDUCATIVO"] == 'Estudios Universitarios').sort("PD", ascending = False)
df_estudos_universitarios.display()


# COMMAND ----------

df_estudos_universitarios_mejores_5 = df_estudos_universitarios.sort("PD", ascending = False).limit(5)
df_estudos_universitarios_peores_5 = df_estudos_universitarios.sort("PD", ascending = True).limit(5)
df_estudios_universitarios_combinados = df_estudos_universitarios_mejores_5.union(
    df_estudos_universitarios_peores_5
)
df_estudios_universitarios_combinados.display()

# COMMAND ----------

# MAGIC %md
# MAGIC PREGUNTA 8

# COMMAND ----------

# MAGIC %md
# MAGIC Tomando la muestra de la pregunta 6 y 7. ¿Quiénes tienen menor probabilidad de
# MAGIC default? (PD). Nota: Si el valor es menor, tiene menor probabilidad.

# COMMAND ----------

df_estudos_universitarios.agg(avg("PD").alias("PROMEDIO_PD_UNIVERSITARIO_TOTAL")).display()
df_estudos_tecnicos.agg(avg("PD").alias("PROMEDIO_PD_TECNICO_TOTAL")).display()
df_estudios_universitarios_combinados.agg(avg("PD").alias("PROMEDIO_PD_UNIVERSIARIO_TO5P")).display()
df_estudos_tecnicos_combinados.agg(avg("PD").alias("PROMEDIO_PD_TECNICO_TOP5")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Respuesta:\
# MAGIC El grupo de estudios universitarios tiene menos probabilidad de default, ya que en promedio tiene un valor menor comparado con el grupo técnico. Si solo analizamos las muestras de los top 5, podemos ver que los que tienen estudios universitarios tienen mejores probabilidad de default, es decir, tienen más probabilidad de endeudarse.

# COMMAND ----------

# MAGIC %md
# MAGIC PREGUNTA 9
# MAGIC
# MAGIC Cree un Dataframe donde solo incluya los campos: ID, Nivel Educativo, Sexo, Edad, PD.
# MAGIC Adicionalmente le solicitan estrezar el portafolio de clientes y su riesgo, por lo que a la
# MAGIC variable Edad se le aplicará un factor en función del ID.
# MAGIC
# MAGIC a. Si el ID es 1, el factor es 0.0001, entonces se multiplicará la Edad +
# MAGIC (Edad*0.0001)
# MAGIC
# MAGIC b. A medida que el ID aumenta en 1, el factor aumenta en la misma proporción,
# MAGIC es decir, para el ID 2, el factor es 0.0002, y así sucesivamente.
# MAGIC
# MAGIC c. La PD va a tener el mismo comportamiento. En el caso que la PD sobrepase el
# MAGIC valor de 1, este ya no sufrirá variación.
# MAGIC
# MAGIC d. Presente el reporte con el cálculo ajustado.

# COMMAND ----------

import pyspark.sql.functions as F

df_clientes_seg = df_clientes_depurado.select('ID', 'NIVEL_EDUCATIVO', 'SEXO', 'EDAD', 'PD')

df_clientes_seg_fil = df_clientes_seg \
            .withColumn('EDAD_MODF', F.col('EDAD') + (F.col('EDAD')*F.col('ID')*0.0001)) \
            .withColumn('PD_MODF', F.when(F.col('PD') <= 1, F.col('PD') + (F.col('PD')*F.col('ID')*0.0001)).otherwise(F.col('PD'))) \
            .withColumn('EDAD_MODF', F.round('EDAD_MODF', 2))\
            .withColumn('PD_MODF', F.round('PD_MODF', 6))
display(df_clientes_seg_fil)

# COMMAND ----------

# MAGIC %md
# MAGIC e. Compárelo con las variables originales. ¿Existe mucha diferencia?

# COMMAND ----------

edad_var = 3
pd_var = 0.05

df_clientes_seg_fil_var = df_clientes_seg_fil.withColumn('VARIACION', F.when((F.col('EDAD_MODF')-F.col('EDAD') >= edad_var) & (F.col('PD_MODF')-F.col('PD') >= pd_var), 1).otherwise(0))
display(df_clientes_seg_fil_var)

# COMMAND ----------

df_clientes_seg_fil_var.filter(F.col('VARIACION') == 1).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Tomando en cuenta como significativo una variación de la edad de 3 años y del PD de 5%, se tiene una modificación de 3314 registros lo cual representa 66% de la muestra total, se puede concluir que si hubo una variación significativa. Igualmente considerar que depende mucho de las variaciones que se tomen para las variables modificadas
