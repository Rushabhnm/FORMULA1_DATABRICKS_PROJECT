# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

results = dbutils.notebook.run("1. ingest_circuits_file_csv",0,{"Data_source":"Ergast_API"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("2. ingest_race_file_csv",0,{"Data_source":"Ergast_API"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("3. ingest_constructor_file_json",0,{"Data_source":"Ergast_API"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("4. ingestion_results_file_json",0,{"Data_source":"Ergast_API"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("5. ingest_drivers_file_json",0,{"Data_source":"Ergast_API"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("6. ingest_pitstops_file_json",0,{"Data_source":"Ergast_API"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("7. ingest_laptimes_file_csv",0,{"Data_source":"Ergast_API"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("8. ingest_qualifying_file_json",0,{"Data_source":"Ergast_API"})

# COMMAND ----------

results
