# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Constructor JSON file
# MAGIC

# COMMAND ----------

dbutils.widgets.text("Data_source","")
v_data_source = dbutils.widgets.get("Data_source")

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1databricksdata/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Making the schema for constructor data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

constructor_schema = StructType(fields=[StructField('constructorId',IntegerType(),False),
                                        StructField('constructorRef',StringType(),False),
                                        StructField('name',StringType(),False),
                                        StructField('nationality',StringType(),False),
                                        StructField('url',StringType(),False)
])
                                                    

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructor_df = spark.read.json(f"{raw_layer}constructors.json", schema=constructor_schema)


# COMMAND ----------


display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Adding columns ingestion_date and extraction_file

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col

# COMMAND ----------

constructor_df_add = add_ingestion_date(constructor_df)

# COMMAND ----------

constructor_df_add = constructor_df_add.withColumn('extraction_file',lit('Constructors'))\
                                        .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(constructor_df_add)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Selecting the required column

# COMMAND ----------

constructor_final_df = constructor_df_add.select(col("constructorId").alias("constructor_id"),col("constructorRef").alias("constructor_ref"),col("name"),col("nationality"),col("ingestion_date"),col("extraction_file"),col("data_source"))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.parquet(f'{processed_layer}constructors',mode="overwrite")

# COMMAND ----------

# Exit command for Notebook
dbutils.notebook.exit("Success")
