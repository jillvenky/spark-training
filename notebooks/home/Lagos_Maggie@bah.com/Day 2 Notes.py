# Databricks notebook source exported at Tue, 11 Aug 2015 20:18:25 UTC
# MAGIC %md
# MAGIC # Day 2 Notes

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrames
# MAGIC 
# MAGIC spark-csv
# MAGIC - CSV not built in, ex. format defined by class ("com.databricks.spark.csv")
# MAGIC - default column type is string  
# MAGIC - more general purpose than splitting by "," among other reasons
# MAGIC - can extend spark-csv to package new data types (ex. as StructType)
# MAGIC 
# MAGIC column
# MAGIC * abstraction in that DataFrame figures out underlying mapping (ex. JSON.field vs CSV[0] vs SQL table.column)
# MAGIC * Python: prefer `df["columnname"]`
# MAGIC * Java: `df.col("first")`
# MAGIC * can be created on the fly via DSL
# MAGIC ```python
# MAGIC df.select(df['firstName'], df['age'], df['age'] > 49, df['age'] + 10]).show(5)
# MAGIC ```
# MAGIC 
# MAGIC DataFrame SQL transformations
# MAGIC * can temporarily name table df.registerTempTable("tablename") to use SQL
# MAGIC * then sqlContext.sql("SELECT ...")
# MAGIC * doesn't do sanitization the way a PreparedStatement would
# MAGIC * df.groupBy doesn't return DataFrame and groupBy("age").count() adds count column to groupBy GroupedData

# COMMAND ----------

# MAGIC %md
# MAGIC # Lambda Functions
# MAGIC 
# MAGIC - is a single statement function intended to be used as argument to other function
# MAGIC - never write a statement with >1 lambda because unreadable
# MAGIC - if lambda requires >1 line, use def

# COMMAND ----------

# MAGIC %md
# MAGIC # Accumulators & Broadcast Variables
# MAGIC ** on certification **
# MAGIC 
# MAGIC * anytime partitions must read/write they're in a conundrum since they don't know about each other
# MAGIC * how to coordinate something between them?
# MAGIC 
# MAGIC #### Accumulator
# MAGIC - one accumulator/task (not necessarily worker or partition)
# MAGIC 
# MAGIC ```python
# MAGIC file = sc.textFile("file.txt")
# MAGIC blankLines = sc.accumulator(0)
# MAGIC 
# MAGIC def showAccumulation(line):
# MAGIC   global blankLines # use outer blankLines
# MAGIC   if (line == ""):
# MAGIC     blankLines +=1                            # cluster receives local copy of external accumulation var
# MAGIC   return line                                 # just return line
# MAGIC 
# MAGIC allLines.map(showAccumulation)
# MAGIC allLines.count                                # everything above is lazy
# MAGIC println("Blank lines: " + blankLines.value)   # gets accumulated value across all tasks
# MAGIC ```
# MAGIC 
# MAGIC #### Broadcast Variable

# COMMAND ----------

# MAGIC %md
# MAGIC # General
# MAGIC 
# MAGIC Tricks
# MAGIC - can bring up pyspark with IPython by setting environment variable IPYTHON=1
# MAGIC - keeps history between deployments and allows for tab-completion

# COMMAND ----------

