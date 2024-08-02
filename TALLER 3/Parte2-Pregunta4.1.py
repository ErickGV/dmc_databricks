# Databricks notebook source
# MAGIC %md
# MAGIC ### Pregunta 4

# COMMAND ----------

#4. Tomando como base la información del blob storage, cree un job que permita encontrar 
# al usuario que usó más el servicio en el 2018. Cree este proceso seccionado (no todo el 
# código en un mismo notebook) y finalmente guarde el resultado en el blob storage

# COMMAND ----------

contenedor_parte_2 = "parte2"

storage_account_name = "grupo4taller3"

storage_account_access_key = "sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2024-07-21T03:20:14Z&st=2024-07-12T19:20:14Z&spr=https&sig=GZgYR03HvRlRVjh%2BofjrmT%2FAU9gyMdgUr54PN1E5Bjk%3D"

spark.conf.set('fs.azure.sas.'+contenedor_parte_2+"."+storage_account_name+'.blob.core.windows.net',storage_account_access_key)

opciones = {"header": "true", "delimiter":",","inferSchema":"true"}
file_location = "wasbs://"+contenedor_parte_2+"@"+storage_account_name+".blob.core.windows.net"


# COMMAND ----------

df = spark.read.format("csv").options(**opciones).load(file_location + "/open_app.txt")

# COMMAND ----------

display(df)

# COMMAND ----------

#Crear una vista temporal a partir del DF

df.createOrReplaceGlobalTempView("users_web")
