# Databricks notebook source exported at Tue, 11 Aug 2015 20:21:09 UTC
# MAGIC %md
# MAGIC # RDD Lab (Python)
# MAGIC 
# MAGIC In this lab, we'll explore some of the RDD concepts we've discussed. We'll be using a data set consisting of reported crimes in Washington D.C. in 2013. This data comes from the [District of Columbia's Open Data Catalog](http://data.octo.dc.gov/). We'll use this data to explore some RDD transitions and actions.
# MAGIC 
# MAGIC ## Exercises and Solutions
# MAGIC 
# MAGIC This notebook contains a number of exercises. If, at any point, you're struggling with the solution to an exercise, feel free to look in the **Solutions** notebook (in the same folder as this lab).
# MAGIC 
# MAGIC ## Let's get started.
# MAGIC 
# MAGIC First, let's import a couple things we'll need.

# COMMAND ----------

from collections import namedtuple
from pprint import pprint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the data
# MAGIC 
# MAGIC The next step is to load the data. Run the following cell to create an RDD containing the data.
# MAGIC 
# MAGIC Note: dbfs is thin wrapper over S3 that Databricks uses. 
# MAGIC base_rdd or input_rdd are usual name for first RDD

# COMMAND ----------

base_rdd = sc.textFile("dbfs:/home/training/wash_dc_crime_incidents_2013.csv")

# COMMAND ----------

# MAGIC %md **Question**: Does the RDD _actually_ contain the data right now?
# MAGIC 
# MAGIC No, lazy loading - equivalent of future (in future/promise paradigm)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the data
# MAGIC 
# MAGIC Let's take a look at some of the data.
# MAGIC 
# MAGIC Note: .take(x) is an action (vs transformation) since it requires actually getting the data and returning to the Driver. Also doesn't necessarily return _first_ 10, but rather first 10 across x many partitions

# COMMAND ----------

for line in base_rdd.take(10):
  print line

# COMMAND ----------

# MAGIC %md Okay, there's a header. We'll need to remove that. But, since the file will be split into partitions, we can't just drop the first item. Let's figure out another way to do it.
# MAGIC 
# MAGIC Note: filter returns an RDD, so it's a transformation (vs action)

# COMMAND ----------

no_header_rdd = base_rdd.filter(lambda line: 'REPORTDATETIME' not in line)

# COMMAND ----------

no_header_rdd.take(10)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 1
# MAGIC 
# MAGIC Let's make things a little easier to handle, by converting the `no_header_rdd` to an RDD containing Python objects.
# MAGIC 
# MAGIC **TO DO**
# MAGIC 
# MAGIC * Split each line into its individual cells.
# MAGIC * Map the RDD into another RDD of appropriate `namedtuple` objects.

# COMMAND ----------

# Replace the <FILL-IN> sections with appropriate code.

# TAKE NOTE: We are deliberately only keeping the first five fields of
# each line, since that's all we're using in this lab. There's no sense
# in dragging around more data than we need.

# Python's namedtuple method creates an object type 'CrimeData' out of the following fields in that order
# helps manipulation
CrimeData = namedtuple('CrimeData', ['ccn', 'report_time', 'shift', 'offense', 'method'])

def map_line(line):
  columns = line.split(',')
  return CrimeData(*columns[:5])

# :5 is a slicing operator, returns items 0-4 as sublist
# * turns list into argument list

data_rdd = no_header_rdd.map(map_line)
for cd in data_rdd.take(5):
  print cd

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 2
# MAGIC 
# MAGIC Next, group the data by type of crime (the "OFFENSE" column).

# COMMAND ----------

grouped_by_offense_rdd = data_rdd.groupBy(lambda data: data.offense)
# doing key extraction
# why does this take a while? some elements never leave partition, may require shuffling (getting data across partitions) which should generally be avoided

# What does this return? You'll need to know for the next step.
for cd in grouped_by_offense_rdd.take(10):
  print cd[0]
  for record in cd[1]:
    print record

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 3
# MAGIC Next, create an RDD that counts the number of each offense. How many murders were there in 2013? How many assaults with a dangerous weapon?

# COMMAND ----------

offense_counts = data_rdd.map(lambda item: (item.offense, item)).countByKey()
# note using .items() 
for offense, counts in offense_counts.items():
  print "{0:30s} {1:d}".format(offense, counts)


# COMMAND ----------

# MAGIC %md ### Question
# MAGIC 
# MAGIC Run the following cell. Can you explain what happened? Is `collectAsMap()` a _transformation_ or an _action_?

# COMMAND ----------

grouped_by_offense_rdd.collectAsMap()
# what's danger here? OutOfMemory (OOM) since collect() and collectAsMap() pulls result in Driver's/local memory

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 4
# MAGIC 
# MAGIC How many partitions does the base RDD have? What about the `grouped_by_offense` RDD? How can you find out?
# MAGIC 
# MAGIC pyspark: getNumParitions()
# MAGIC 
# MAGIC jvm: partitions.length
# MAGIC 
# MAGIC **Hint**: Check the [API documentation](http://localhost:8080/~bmc/spark-1.4.1-docs/api/python/pyspark.html#pyspark.RDD).

# COMMAND ----------

print base_rdd.getNumPartitions()
print grouped_by_offense_rdd.getNumPartitions()

# COMMAND ----------

print base_rdd.getNumPartitions()
print grouped_by_offense_rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 5
# MAGIC 
# MAGIC Since we're continually playing around with this data, we might as well cache it, to speed things up.
# MAGIC 
# MAGIC **Question**: Which RDD should you cache? 
# MAGIC 
# MAGIC 1. `base_rdd`
# MAGIC 2. `no_header_rdd`
# MAGIC 3. `data_rdd`
# MAGIC 4. None of them, because they're all still too big.
# MAGIC 5. It doesn't really matter.

# COMMAND ----------

data_rdd.cache()

# COMMAND ----------

# SOLUTION

# It's okay to cache the base RDD. It's not very large. Proof:
print("Count of lines: {0}".format(base_rdd.count()))
print("Count of characters (Unicode): {0}".format(base_rdd.map(lambda line: len(line)).reduce(lambda a, b: a + b)))

# They're all about the same size, since we didn't filter any data out. However,
# since we're mostly working with the `data_rdd`, that's the one to cache.
data_rdd.cache()

# Should generally cache the farthest down the tree possible, one likely to have most children / most filtered RDD in chain
# Is cache an action or transformation? it's lazy - transformation 
# What does caching actually mean? equivalent to persist in memory only (doesn't spill to disk)
# What if cache > JVM? If it runs out of memory, throws away some of the data. So a later RDD will have to re-run DAG to fill in missing data
# How can you uncache? No direct implementation but since cache operates over persist, use unpersist


# COMMAND ----------

# MAGIC %md ### Exercise 6
# MAGIC 
# MAGIC Display the number of homicides by weapon (method).

# COMMAND ----------

result_rdd1 = data_rdd.filter(lambda item: item.offense == 'HOMICIDE')\
            .map(lambda item: (item.method, 1))\
            .reduceByKey(lambda i, j: i+j)

# would generally reduceByKey because it does partial sums on the cluster
# vs groupByKey() and then reduce because groupByKey sends everything across cluster
# reduceByKey is optimized to group by followed by reduce

# why not just countByKey()? reduceByKey is a transformation and allows you to do any transformation
    
# BONUS: Make the output look better, using a for loop.
for method, counts in result_rdd1.collect():
  print "{0:30s} {1:d}".format(method, counts)

# COMMAND ----------

# SOLUTION
result_rdd1 = data_rdd.filter(lambda item: item.offense == 'HOMICIDE').\
                       map(lambda item: (item.method, 1)).\
                       reduceByKey(lambda i, j: i + j)

for method, count in result_rdd1.collect():
  print "{0:10s} {1:d}".format(method, count)
  
# OR
result_rdd2 = data_rdd.filter(lambda item: item.offense == 'HOMICIDE')\
            .map(lambda item: (item.method, 1)).countByKey()
  # note: countByKey() returns dictionary vs RDD
for method, counts in result_rdd2.items():
  print "{0:30s} {1:d}".format(method, counts)


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 7
# MAGIC 
# MAGIC During which police shift did the most crimes occur in 2013?

# COMMAND ----------

# Hint: Start with the data_rdd
print data_rdd.map(lambda item: (item.shift, 1))\
.reduceByKey(lambda c1, c2: c1+c2)\
.max(key=lambda t: t[1])

# COMMAND ----------

# MAGIC %md ### Demonstration
# MAGIC 
# MAGIC Let's plot murders by month. DataFrames are useful for this one.

# COMMAND ----------

# MAGIC %md To do this property, we'll need to parse the dates. That will require knowing their format. A quick sampling of the data will help.

# COMMAND ----------

data_rdd.map(lambda item: item.report_time).take(30)

# COMMAND ----------

# MAGIC %md Okay. We can now create a [`strptime()` format string](https://docs.python.org/2.7/library/datetime.html?highlight=strptime#datetime.datetime.strptime), allowing us to use the `datetime.datetime` Python class to parse those strings into actual datetime values.

# COMMAND ----------

from datetime import datetime
date_format = "%m/%d/%Y %H:%M:%S %p"


# COMMAND ----------

# MAGIC %md Now, we can create the data frame. We'll start with the `no_header_rdd` and map it slightly differently than we did to create `data_rdd`:

# COMMAND ----------

from pyspark.sql.types import *
def make_row(line):
  columns = line.split(",")[:5]
  columns[1] = datetime.strptime(columns[1], date_format)
  return CrimeData(*columns)

df = no_header_rdd.map(make_row).toDF()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md Let's use a user-defined function (something supported by DataFrames and Spark SQL) to give us a `month` function we can use to extract just the month part of a `Timestamp`.

# COMMAND ----------

month = udf(lambda dt: dt.month)

# COMMAND ----------

display( 
  df.filter(df['offense'] == 'HOMICIDE')
    .select(month(df['report_time']).alias("month"), df['offense'])
    .groupBy('month').count()
)

# COMMAND ----------

# MAGIC %md What about all crimes per month?

# COMMAND ----------

display(
  df.select(month(df['report_time']).alias('month'))
    .groupBy('month').count()
)

# COMMAND ----------

