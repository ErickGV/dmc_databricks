# Databricks notebook source
#Vamos a crear una BBDD para luego integrarlo a nuestro delta lake

db = "s09_db6_erickgv"

#Crear la base de datos

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

spark.sql(f"USE {db}")

#Preparar nuestra base de datos y configurar para habilitar DL
spark.sql(" SET spark.databricks.delta.formatCheck.enable = false ")
spark.sql(" SET spark.databricks.delta.properties.autoOptimize.optimizeWrite = true ")

# COMMAND ----------

# Declarar variables para la conexión con la base de datos postgre

driver = "org.postgresql.Driver"
database_host = "databricksdmc.postgres.database.azure.com"
database_port = "5432"
database_name = "northwind"
database_user = "grupo6_antony"
password = "Especializacion6"
table = "orders"

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

# COMMAND ----------

#Leemos una tabla de postgre

sql_order = (spark.read.format("jdbc").option("driver",driver).option("url",url).option("dbtable",table).option("user",database_user).option("password",password).load())

# COMMAND ----------

sql_order.display()

# COMMAND ----------

tables = ["orders","customer_customer_demo","customers","employees", "order_details", "region","territories","suppliers"]

# COMMAND ----------

sql_order = {table: spark.read.format("jdbc")
             .option("driver", driver)
             .option("url", url)
             .option("dbtable", table)
             .option("user", database_user)
             .option("password", password)
             .load() for table in tables}

# COMMAND ----------

sql_order["orders"].display()

# COMMAND ----------

#Para escribir una tabla
sql_order.write.format("delta").mode("overwrite").saveAsTable(f"{db}.orders")
3

# COMMAND ----------

for table in tables:
    options = {'driver':driver, "url":url,"dbtable":table,"user":database_user,"password":password }
    dt_table = spark.read.format("jdbc").options(**options).load()
    table = "{db}.{table}"
    dt_table.write.format("delta").mode("overwrite").saveAsTable(table)

# COMMAND ----------

# Para mejorar el proceso
import os

tables = ["orders", "customer_customer_demo", "customers", "employees", "order_details", "region", "territories", "suppliers"]

# Crear una carpeta temporal para almacenar archivos intermedios
temp_dir = os.path.join("/tmp/deltaesperickgv", f"delta_tables_{os.getpid()}")
os.makedirs(temp_dir, exist_ok=True)

for table in tables:
    # Leer datos en un DataFrame
    df = spark.read.format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", database_user) \
        .option("password", password) \
        .load()

    # Guardar DataFrame como tabla Delta en la carpeta temporal
    df.write.format("delta").mode("overwrite").save(os.path.join(temp_dir, table))

# Cargar tablas Delta del directorio temporal a la base de datos
for table in tables:
    spark.read.format("delta").load(os.path.join(temp_dir, table)).write.format("delta").mode("overwrite").saveAsTable(f"{db}.{table}")

# Eliminar la carpeta temporal
os.removedirs(temp_dir)

# COMMAND ----------

path_orders = f'dbfs:/user/hive/warehouse/{db}.db/orders'
df_orders = spark.read.format("delta").load(path_orders)
display(df_orders)
