# Databricks notebook source exported at Wed, 12 Aug 2015 21:00:45 UTC
# MAGIC %md
# MAGIC #This is the Accumulator Lab Exercise #1 in Python

# COMMAND ----------

# MAGIC %md ##The first thing we need is to wrap a file with the SparkContext textFile(...) method, which will give us an RDD of Strings (lines of the file)... Use the /databricks-datasets/README.md file provided for you on the Databricks server as a starting point.

# COMMAND ----------

file = sc.textFile("dbfs:/databricks-datasets/README.md")


# COMMAND ----------

# MAGIC %md ##Next we will need to create an Accumulator using the SparkContext Factory [GoF] method accumulator(...) which takes an initial value as the argument. Set the initial value to zero. We will use this accumulator to count the number of blank lines in the README.md file.

# COMMAND ----------

blankLines = sc.accumulator(0)

# COMMAND ----------

# MAGIC %md ##Next we need the method which will inspect each incoming line from the RDD and determine whether or not it is in fact a blank line. This will allow us to add one to the Accumulator when we find a blank line.

# COMMAND ----------

def showAccumulation(line):
  global blankLines   # use outer blankLines above
  if (line == ""): blankLines += 1
#
# 	Check to see if the incoming line is blank
#   If you find a blank line add one to the Accumulator
#
  return line

allLines = file.flatMap(showAccumulation)
allLines.count()
print "Blank lines: %d" % blankLines.value


# COMMAND ----------

# MAGIC %md #This is the Broadcast Variables Lab Exercise #1 in Python

# COMMAND ----------

# MAGIC %md ##Using the SparkContext and the two methods on the context, broadcast(...) and parallelize(...) create a value to add on to any given name in a list, such as "... is really wonderful!" and then create a list of names.

# COMMAND ----------

theVar = sc.broadcast('... is really wonderful!') # use the SparkContext to broadcast some value ...
namesRDD = sc.parallelize(['mr.beans','peter','bjorn','john']) # use the SparkContext to create a list of names, you will filter the names later

def showBroadcast(name):
  return name + theVar.value; # concatenate the incoming name with the broadcast value ...

someNames = namesRDD.filter(lambda name: 'o' in name) # filter out names that don't have the part you like

print someNames.map(showBroadcast).collect() # we collect here because otherwise the output will be on the cluster we want to see output here ...

# COMMAND ----------

