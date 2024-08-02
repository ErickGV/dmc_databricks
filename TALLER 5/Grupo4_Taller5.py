# Databricks notebook source
sas_token = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-08-05T10:42:07Z&st=2024-07-26T02:42:07Z&spr=https&sig=CFszBjZ2LxVPLfwsLfd%2BjjHPBOcCIEi3KfDNvhG3Zhw%3D'
storage = 'grupo4taller5'
input_container = 'input'

conexion = f"fs.azure.sas.{input_container}.{storage}.blob.core.windows.net"

# Configuración del acceso con SAS token
spark.conf.set(
    conexion,
    sas_token
)

input_container_ruta = f'wasbs://'+input_container+'@'+storage+'.blob.core.windows.net'

# COMMAND ----------

dbutils.fs.ls(input_container_ruta)

# COMMAND ----------

opciones = {"header": "true", "delimiter": ";", "inferSchema": "true"}
colegios_df = spark.read.format("csv").options(**opciones).load(f"{input_container_ruta}/Colegios.csv")

# COMMAND ----------

display(colegios_df)

# COMMAND ----------

colegios_df_filt = colegios_df.select("CODLOCAL","CEN_EDU", "D_NIV_MOD", "D_FORMA", "D_COD_CAR", "D_TIPSSEXO", "D_GESTION", "D_GES_DEP", "DIRECTOR", "TELEFONO", "EMAIL","PAGWEB","DIR_CEN", "CEN_POB", "DAREACENSO", "D_DPTO", "D_PROV", "D_DIST", "D_REGION", "NLAT_IE", "NLONG_IE", "CODOOII","D_DREUGEL", "D_COD_TUR", "ESTADO", "TALUM_HOM", "TALUM_MUJ", "TDOCENTE", "TSECCION")

# COMMAND ----------

display(colegios_df_filt)

# COMMAND ----------

db = "grupo4_taller5"
table = "colegios"
#Crear la base de datos
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"DROP TABLE IF EXISTS {db}.{table}")
spark.sql(f"USE {db}")

#Preparar nuestra base de datos y configurar para habilitar DL
spark.sql(" SET spark.databricks.delta.formatCheck.enable = false ")
spark.sql(" SET spark.databricks.delta.properties.autoOptimize.optimizeWrite = true ")

# COMMAND ----------

colegios_df_filt.write.format("delta").mode("overwrite").saveAsTable(f"{db}.{table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from grupo4_taller5.colegios
