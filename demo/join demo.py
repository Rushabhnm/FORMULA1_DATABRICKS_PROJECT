# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

display(raw_layer)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_layer}circuits")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_layer}races").filter("race_year IN (2020,2021)")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

df_join = circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_ID)\
    .select(circuits_df.circuit_id, circuits_df.circuit_ref,circuits_df.name,circuits_df.location, circuits_df.country,races_df.race_ID,races_df.name.alias("race_name"),races_df.round, races_df.race_timestamp, races_df.race_year)

# COMMAND ----------

display(df_join)

# COMMAND ----------

display(df_join.orderBy("race_year","circuit_id","round"))

# COMMAND ----------

df_join_left = circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_ID,"left")\
    .select(circuits_df.circuit_id, circuits_df.circuit_ref,circuits_df.name,circuits_df.location, circuits_df.country,races_df.race_ID,races_df.name.alias("race_name"),races_df.round, races_df.race_timestamp, races_df.race_year)

# COMMAND ----------

display(df_join_left)