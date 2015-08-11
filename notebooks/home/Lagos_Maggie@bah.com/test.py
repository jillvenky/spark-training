# Databricks notebook source exported at Tue, 11 Aug 2015 13:58:21 UTC
x = 10
print x

# COMMAND ----------

# MAGIC %md
# MAGIC # Design Patterns
# MAGIC 
# MAGIC #### Factory - object creation w/o calling constructor
# MAGIC ```java
# MAGIC CarFactory factory = new CarFactory();
# MAGIC Car c = factory.getInstance("BMW");
# MAGIC ```
# MAGIC * often pass parameter into factory to get instance
# MAGIC 
# MAGIC #### Fluent - chaining
# MAGIC `c.setEngine("v8").setMake("Roadster").setColor("Yellow")`
# MAGIC 
# MAGIC #### Observer - get notified when something changes state
# MAGIC * Some observeable has a List<Subscribers>
# MAGIC * Subscriber will execute whatever its notify() specifies

# COMMAND ----------

# MAGIC %md
# MAGIC # Applying them in Spark
# MAGIC 
# MAGIC ```java
# MAGIC List<String> names = new ArrayList<String>();
# MAGIC names.add("Amy")
# MAGIC 
# MAGIC // parallelize gives data to the cluster
# MAGIC // rddNames is reference to data on Spark's cluster
# MAGIC JavaRDD<String> rddNames = context.parallelize(names);
# MAGIC 
# MAGIC // functional programming example
# MAGIC // foreachPartition will push each String element in rdd to the function in parameter
# MAGIC JavaRDD<String> rddNamesAsParition = rddNames.foreachPartition(
# MAGIC   // partition function is waiting to see what foreachPartition receives 
# MAGIC   new PartitionFunction(
# MAGIC     // holds "notify" aka "call" in Spark
# MAGIC     // this function goes from driver to cluster, so must be serializable
# MAGIC   )
# MAGIC );
# MAGIC ```
# MAGIC 
# MAGIC * context.parallelize(names) - context is a _factory_ returning a Spark object
# MAGIC * foreachPartition returns the same type as RDD (JavaRDD<String>) using _fluent_ programming
# MAGIC * PartitionFunction is the _subscriber_ observing the RDD

# COMMAND ----------

# MAGIC %md
# MAGIC # DataFrames
# MAGIC 
# MAGIC - fundamentally tied to Spark SQL, using Catalyst under the covers
# MAGIC - to use with Hive, must build Spark w/ Hive support (only a couple command line switches to build). Can also use HiveContext without metastore (better SQL parser)
# MAGIC - DataFrames use RDDs, but lay a schema over it that mimic a table in a distributed DB (rows & columns)
# MAGIC - like RDDs, actions execute a query **but** transformations to a DataFrame are accepted by the query optimizer which produces the RDDs & optimized physical query plan (a level removed from RDDs)
# MAGIC - **significantly faster than RDDs and similar performance across languages**
# MAGIC - Supports Parquet, JSON, Hive, S3 & external, ex. elasticsearch
# MAGIC - to use with Hive, import HiveContext

# COMMAND ----------

# MAGIC %md
# MAGIC # General
# MAGIC 
# MAGIC 
# MAGIC - flatMap when you need collection vs Map when you need an object
# MAGIC - action (ex. saveAsTextFile(), count()) vs transformation (returns RDD, generally immutable)
# MAGIC - spark-packages.org to check for built-in data sources

# COMMAND ----------

# MAGIC %md
# MAGIC #Day 1 Review
# MAGIC 
# MAGIC ## Vocab
# MAGIC 
# MAGIC #### RDD
# MAGIC - Resilient Distributed Dataset
# MAGIC - distributed collection of records of any type
# MAGIC - distributed across partitions, partitions allow for additional parallelization
# MAGIC - returned by transformations
# MAGIC 
# MAGIC #### Transformation
# MAGIC - creates child RDD from parent. Parent is unchanged
# MAGIC - how to provide functionality? pass a function which must implement serializable
# MAGIC - lazily done
# MAGIC - examples: filter, map
# MAGIC 
# MAGIC #### Action
# MAGIC - trigger data read and transformations to run
# MAGIC - Examples: count, collect
# MAGIC - Warning: collect can cause OutOfMemory error if pulling too much data back to driver (application)
# MAGIC 
# MAGIC #### Lazy?
# MAGIC - computation doesn't take place until action executes (including reading initial data and creating base RDD)
# MAGIC - no eager mode
# MAGIC 
# MAGIC #### DAG
# MAGIC - Directed Acyclic Graph
# MAGIC - Abstraction - chain of tasks 
# MAGIC 
# MAGIC #### Caching
# MAGIC - cache a particular RDD so susequent actions don't need to re-run entire DAG
# MAGIC - if you cache, won't pull from backend (might be out of date)
# MAGIC - any auto caching? yes, shuffle files are used temporarily so Spark doesn't have to recompute
# MAGIC - is it lazy? yes. and returns an RDD so it's a transformation
# MAGIC 
# MAGIC ------------------
# MAGIC 
# MAGIC ## Functional Programming
# MAGIC 
# MAGIC #### Two main aspects
# MAGIC - immutability (facilitates concurrency, i.e. less thread context switching)
# MAGIC - functions should return values that are based only on passed in data
# MAGIC - no synchronization
# MAGIC - easier to reason, test, debug
# MAGIC 
# MAGIC #### Another fad?
# MAGIC - no, distributed computing
# MAGIC - allows to scale out
# MAGIC 
# MAGIC ------------------
# MAGIC 
# MAGIC ## DataFrame
# MAGIC 
# MAGIC #### What?
# MAGIC - abstraction layer over RDD that adds schema
# MAGIC - data in rows & named columns
# MAGIC 
# MAGIC #### Schema?
# MAGIC - rows w/ named columns
# MAGIC - columns have known type
# MAGIC 
# MAGIC #### DataFrame vs RDD
# MAGIC - Faster
# MAGIC - Optimization happens on query
# MAGIC - Query plan created (not DAG) by Catalyst Optimizer
# MAGIC - same: implemented with DAG, lazy transformations, triggering actions
# MAGIC 
# MAGIC #### When to use RDD vs DataFrame?
# MAGIC - schema-less data. 
# MAGIC 
# MAGIC #### How to tell Spark about schema? 
# MAGIC - via namedtuple, Row object, Java bean/reflection
# MAGIC 
# MAGIC #### DataFrames data types
# MAGIC - JSON, HDFS, Hive, MySQL, Parquet (self describing columnar)
# MAGIC - Plugins: Cassandra, ES
# MAGIC - check spark-pacakges.org for more

# COMMAND ----------

