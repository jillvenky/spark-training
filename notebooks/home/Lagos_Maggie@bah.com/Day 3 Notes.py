# Databricks notebook source exported at Wed, 12 Aug 2015 21:05:12 UTC
# MAGIC %md
# MAGIC 
# MAGIC # Day 3 Notes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Certification Prep
# MAGIC 
# MAGIC watch Paco Nathan Spark video
# MAGIC 
# MAGIC Base RDD
# MAGIC - first created RDD, base of DAG
# MAGIC - when does it get created? when triggered by action
# MAGIC 
# MAGIC Transformation vs Action
# MAGIC - transformation returns RDD, action doesn't
# MAGIC 
# MAGIC map vs flatmap
# MAGIC - differs in return type of call (T t), both return RDD
# MAGIC - map acts on each element of RDD
# MAGIC - flatmap flattens elements into Collection
# MAGIC 
# MAGIC lazy eval
# MAGIC - chain of RDD transformations create DAG
# MAGIC - Spark optimizes DAG and evaluation is forced by action
# MAGIC 
# MAGIC transformations and shuffle
# MAGIC - aka wide
# MAGIC - which cause shuffle? repartition(num), groupByKey(...), etc
# MAGIC 
# MAGIC toDebugString
# MAGIC - prints DAG at and above, shows shuffles
# MAGIC 
# MAGIC cache
# MAGIC - fault tolerant
# MAGIC - up to 10x faster than not cacacehd
# MAGIC - storage level: memory only (can specify other)
# MAGIC 
# MAGIC partitioning
# MAGIC - sometimse have to specify partitioner
# MAGIC - known subclasses: Hash, RangePartitioner
# MAGIC 
# MAGIC cluster sizing
# MAGIC - not limited - 0-8k nodes
# MAGIC 
# MAGIC spark streaming
# MAGIC - consumes slot in executor
# MAGIC - create discretized/DStreams via StreamingContext Factory 
# MAGIC - one RDD/batch interval
# MAGIC - stateless vs stateful? stateful tracks data across time
# MAGIC - recovering requires stateful
# MAGIC 
# MAGIC Error conditions
# MAGIC - functions passed to executor must implement Serializable (in python, must be able to be pickled)
# MAGIC 
# MAGIC reduceByKey
# MAGIC - setting # of reducers? parameter after reduce function
# MAGIC - x reducers = x part files = x workers
# MAGIC 
# MAGIC does Spark require more heap space than Hadoop?
# MAGIC - no
# MAGIC 
# MAGIC immutable RDDs?
# MAGIC - yes, but some exceptions...
# MAGIC - if a Partition is lost and requires getting new data from a mutable source (ex db)
# MAGIC - it _can_ be mutable if a function passed into map(...) modifies RDD Objects -- but avoid this! Goes against functional (programming) paradigm
# MAGIC 
# MAGIC other
# MAGIC - comment blocks on code are important when reviewing slides
# MAGIC - all Scala, Python, & Java are fair game
# MAGIC - and Spark Streaming, Spark SQL, Graph X
# MAGIC - try http://training.databricks.com/workshop/itas_workshop.pdf 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark Streaming
# MAGIC 
# MAGIC #### example
# MAGIC JavaDStream =RDD/batch interval
# MAGIC 
# MAGIC rdd.print()
# MAGIC - prints entire RDD contents to stream's console
# MAGIC - prints every batchInterval seconds
# MAGIC - transformations operate on every RDD in batchInterval seconds
# MAGIC 
# MAGIC receiver thread
# MAGIC - 1 receiver thread/incoming stream that then distributes across cluster
# MAGIC - splits streaming data into time-based discretized blocks/RDDs
# MAGIC - ex. if batchInterval is 5 seconds, an RDD created every 5 secs
# MAGIC - recommended batch interval = 1 second
# MAGIC - redundant blocks are also created
# MAGIC 
# MAGIC getting the stream
# MAGIC - Kafka helps, acting as buffer
# MAGIC - only 1 streaming context/JVM but can get as many streams as you want until context is closed
# MAGIC 
# MAGIC ssc.start (Scala)
# MAGIC - must be followed by sc.awaitTermination for a standalone program
# MAGIC - will start streaming thread in background, but will only return results in shell unless ^^

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explaining RDD.aggregate(...)
# MAGIC - seqOp = per partition function, ex. partial sum
# MAGIC - combOp = combining each partitions seqOp, ex. computes total sum

# COMMAND ----------

rdd = sc.parallelize(range(0,1000), 10)
print rdd.getNumPartitions()
rdd.sum() #implemented with aggregate

# find sum of RDD
seqOp = lambda x, y: x+y
combOp = seqOp
print rdd.aggregate(0,seqOp,combOp)

# let's concatenate on each partition (as - separated string) then combine the partition items w/ newlines
seqOp2 = lambda x, y: str(x)+"-"+str(y)
combOp2 = lambda x, y: str(x)+"\n"+str(y)
print rdd.aggregate('', seqOp2, combOp2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### GraphX
# MAGIC 
# MAGIC - will be a scala question
# MAGIC - practiced in lab
# MAGIC - focus on before DMZ for cert

# COMMAND ----------

# MAGIC %md
# MAGIC ## Machine Learning
# MAGIC 
# MAGIC Feature vector
# MAGIC - must featurize data (ex. log text)
# MAGIC - what features, what weights?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Architecture
# MAGIC 
# MAGIC Run spark on
# MAGIC - local, standalone scheduler (static # of executors)
# MAGIC - YARN, Mesos (dynamic #)
# MAGIC 
# MAGIC Standalone
# MAGIC - workers have executors, executors have tasks
# MAGIC - different workers can have different cores (spark-env.sh)
# MAGIC - can have multiple masters
# MAGIC   - high availability via ZooKeeper
# MAGIC   - can be added live
# MAGIC 
# MAGIC YARN
# MAGIC - fine grained tuneability, ex. can request specific nodes
# MAGIC 
# MAGIC Modes
# MAGIC - cluster mode, client can be on Node Manager itself
# MAGIC - client mode, client is outside cluster

# COMMAND ----------

