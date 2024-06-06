# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType, StringType

# COMMAND ----------

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# COMMAND ----------

# Create dummy data
data = [(1, "Rushabh", 25, "Chicago"),
        (2, "Siddhant", 25, "Mumbai"),
        (3, "Taher", 25, "Mumbai"),
        (4, "Vineet", 24, "Toronto"),
        (5, "Darshil", 24, "Toronto"),]

# COMMAND ----------

df = spark.createDataFrame(data, schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").save(f"{delta_tables}friends")