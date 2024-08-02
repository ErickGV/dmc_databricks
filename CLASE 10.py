# Databricks notebook source
#https://adb-2802698215383577.17.azuredatabricks.net#secret/createScope
#dominio de databricks . #secret/createScope

# COMMAND ----------

dbutils.secrets.list(scope="scope_erick_gonzales")

# COMMAND ----------

password = dbutils.secrets.get(scope="scope_erick_gonzales", key="db-posgres")

# COMMAND ----------

driver = "org.postgresql.Driver"
database_host = "databricksdmc.postgres.database.azure.com"
database_port = "5432"
database_name = "northwind"
database_user = "grupo6_antony"
table = "orders"

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

# COMMAND ----------

sql_order = (spark.read.format("jdbc").option("driver",driver).option("url",url).option("dbtable",table).option("user",database_user).option("password",password).load())

# COMMAND ----------

sql_order.display()
