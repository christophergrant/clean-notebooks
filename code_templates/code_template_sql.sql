-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Define all of the parameters for this code.  The output parameter is required - the Cleanroom needs to know where to write the results

-- COMMAND ----------

--REMOVE WIDGET output;
CREATE WIDGET TEXT output DEFAULT "";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Perform all of your computations.  This is a SQL example, so you can use temporarly views do do multiple computational steps.  They can all be in one cell or in many cells

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW myview AS
SELECT a.user, a.ranking, b.second_ranking
FROM snap.test1 a, fakeadvertiser.test1 b
WHERE b.user = a.user

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write out the results.  Please use this syntax for SQL - it will create the Delta table location on storage with the appropriate schema if it does not already exist, and then it will insert the results.  You can also use a MERGE statement here instead of an insert

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS delta.`$output` AS
SELECT * FROM myview
LIMIT 0;
INSERT INTO delta.`$output`
SELECT * FROM myview;
