# Databricks notebook source
# MAGIC %md
# MAGIC ### Define all of the parameters for this code.  The output parameter is required - the Cleanroom needs to know where to write the results

# COMMAND ----------

dbutils.widgets.text("output", "")

# COMMAND ----------

output = dbutils.widgets.get("output")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform all of your computations.  They can all be in one cell or in many cells

# COMMAND ----------

snapTest1Df = spark.read.table("snap.test1")
advTest1Df = spark.read.table("fakeadvertiser.test1")
outputDf = snapTest1Df.alias("a").join(advTest1Df.alias("b"), snapTest1Df.user == advTest1Df.user, "inner").select("a.user", "a.ranking", "b.second_ranking")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write out the results.  If the Delta table doesn't already exist it will create it.  You can also do a merge or an overwrite

# COMMAND ----------

outputDf.write.format("delta").mode("append").save(output)
