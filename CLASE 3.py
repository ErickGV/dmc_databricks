# Databricks notebook source
#test
df_ventas = spark.read.table("ventas_erick_gonzales")

# COMMAND ----------

df_ventas.display()

# COMMAND ----------

df_ventas.select("tienda", "color", "precio_venta").show()

# COMMAND ----------

df_ventas = df_ventas.withColumn("precio_venta_usd", df_ventas["precio_venta"] / 3.71)
df_ventas.display()

# COMMAND ----------


