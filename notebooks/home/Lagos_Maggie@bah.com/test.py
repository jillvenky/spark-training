# Databricks notebook source exported at Mon, 10 Aug 2015 17:03:17 UTC
x = 10
print x

# COMMAND ----------

# MAGIC %md
# MAGIC # Design Patterns
# MAGIC 
# MAGIC #### Factory - object creation w/o calling constructor
# MAGIC ```CarFactory factory = new CarFactory();
# MAGIC Car c = factory.getInstance("BMW");```
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
# MAGIC #Applying them in Spark
# MAGIC 
# MAGIC ```java
# MAGIC List<String> names = new ArrayList<String>();
# MAGIC names.add("Amy")```
# MAGIC 
# MAGIC ```java
# MAGIC // parallelize gives data to the cluster
# MAGIC // rddNames is reference to data on Spark's cluster
# MAGIC JavaRDD<String> rddNames = context.parallelize(names);
# MAGIC ```
# MAGIC 
# MAGIC ```java
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

