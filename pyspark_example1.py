orders = sc.textFile("file:///home/ubuntu/data/retail_db/orders")
help(orders.map)
#Get status
orders.map(lambda o: o.split(",")[3]).first()
#Get count
orders.map(lambda o: o.split(",")[1]).first()

#Convert date format from YYYY-MM-DD HH24:MI:SS -> YYYYMM
#Type cast date to integer
orders.map(lambda o: int(o.split(",")[1].split(" ")[0].replace("-", ""))).first()
orders.map(lambda o: int(o.split(",")[1].split(" ")[0].replace("-", ""))).take(10)
orders.map(lambda o: int(o.split(",")[1].split(" ")[0].replace("-", ""))).count()
orderItems=sc.textFile("file:///home/ubuntu/data/retail_db/order_items/")

#Create tuples
orders.map(lambda o: (o.split(",")[3], 1))

orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items/")
orderItems.first()
for i in orderItems.take(10): print(i)
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
orderItemsMap.first()
for i in orderItemsMap.take(10): print(i)

filter
orders = sc.textFile("file:///home/ubuntu/data/retail_db/orders")
ordersComplete = orders. \
filter(lambda o: 
  o.split(",")[3] in ["COMPLETE", "CLOSED"] and o.split(",")[1][:7] == "2014-01")
  
  
  join
  #joins
orders = sc.textFile("file:///home/ubuntu/data/retail_db/orders")
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items")

ordersMap = orders. \
map(lambda o:(int(o.split(",")[0]), o.split(",")[1]))

orderItemsMap = orderItems. \
map(lambda oi:(int(oi.split(",")[1]), float(oi.split(",")[4])))

ordersJoin = ordersMap.join(orderItemsMap)

for i in ordersJoin.take(10): print(i)


outer join
#outer join
orders = sc.textFile("/public/retail_db/orders")
orderItems = sc.textFile("/public/retail_db/order_items")

ordersMap = orders. \
map(lambda o:(int(o.split(",")[0]), o.split(",")[3]))

orderItemsMap = orderItems. \
map(lambda oi:(int(oi.split(",")[1]), float(oi.split(",")[4])))

ordersLeftOuterJoin = ordersMap.leftOuterJoin(orderItemsMap)

ordersLeftOuterJoinFilter = ordersLeftOuterJoin. \
filter(lambda o: o[1][1] == None)

for i in ordersLeftOuterJoin.take(10): print(i)

ordersRightOuterJoin = orderItemsMap.rightOuterJoin(ordersMap)
ordersRightOuterJoinFilter = ordersRightOuterJoin. \
filter(lambda o: o[1][0] == None)

for i in ordersRightOuterJoinFilter.take(10): print(i)


aggregations
Aggregations - total
orderItems = sc.textFile("/public/retail_db/order_items")
orderItems.count()

#Aggregations - total - Get revenue for given order_id
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items")
orderItemsFiltered = orderItems. \
filter(lambda oi: int(oi.split(",")[1]) == 2)
orderItemsSubtotals = orderItemsFiltered. \
map(lambda oi: float(oi.split(",")[4]))
orderItemsSubtotals = orderItems. \
map(lambda o: int(o.split(",")[1]), float(o.split(",")[4]))

int(oi.split(",")[1])

from operator import add
# orderItemsSubtotals.reduce(add)
orderItemsSubtotals.reduce(lambda x, y: x + y)

countby status
#Get count by status - countByKey
orders = sc.textFile("file:///home/ubuntu/data/retail_db/orders")

ordersStatus = orders. \
map(lambda o: (o.split(",")[3], 1))

countByStatus = ordersStatus.countByKey()
for key,value in countByStatus.items(): print(key,value)

groupbykey:
#Get revenue for each order_id - groupByKey
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items")

orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

orderItemsGroupByOrderId = orderItemsMap.groupByKey()
revenuePerOrderId = orderItemsGroupByOrderId. \
map(lambda oi: (oi[0], round(sum(oi[1]), 2)))

for i in revenuePerOrderId.take(10): print(i)

# Get order item details which have minimum order_item_subtotal for given order_id
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items")

orderItemsFiltered = orderItems. \
filter(lambda oi: int(oi.split(",")[1]) == 2)
orderItemsFiltered. \
reduce(lambda x, y: 
       x if(float(x.split(",")[4]) < float(y.split(",")[4])) else y
      )
There are several APIs to perform aggregations

By Key aggregations
countByKey
reduceByKey
aggregateByKey
groupByKey can be used for aggregations but should be given low priority as it does not use a combiner
Using countByKey
#Get count by status - countByKey
orders = sc.textFile("file:///home/ubuntu/data/retail_db/orders")

ordersStatus = orders. \
map(lambda o: (o.split(",")[3], 1))

countByStatus = ordersStatus.countByKey()
for i in countByStatus: print(i)

Using groupByKey
groupByKey can be used for any aggregation
It is least preferred as combiner will not be used
groupByKey is a generic API which group values into an array for a given key
On top of aggregations, we can perform many other transformations using groupByKey


#Get revenue for each order_id - groupByKey
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items")

orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

orderItemsGroupByOrderId = orderItemsMap.groupByKey()
revenuePerOrderId = orderItemsGroupByOrderId. \
map(lambda oi: (oi[0], round(sum(oi[1]), 2)))

#Get order item details in descending order by revenue - groupByKey
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items")

orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), oi))
orderItemsGroupByOrderId = orderItemsMap.groupByKey()

orderItemsSortedBySubtotalPerOrder = orderItemsGroupByOrderId. \
flatMap(lambda oi: 
  sorted(oi[1], key=lambda k: float(k.split(",")[4]), reverse=True))
for i in orderItemsSortedBySubtotalPerOrder.take(10): print(i)

Using reduceByKey
reduceByKey uses combiner
It is used when logic to compute intermediate values and logic to compute the final value using intermediate values are same
It is very straightforward to implement
It takes one anonymous or lambda function with 2 arguments


#Get revenue for each order_id - reduceByKey
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

from operator import add
revenuePerOrderId = orderItemsMap. \
reduceByKey(add)

#Alternative way of adding for each key using reduceByKey
revenuePerOrderId = orderItemsMap. \
reduceByKey(lambda x,y:x+y)

#Get order item id with minimum revenue for each order_id - reduceByKey
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items/")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

minSubtotalPerOrderId = orderItemsMap. \
reduceByKey(lambda x, y: x if(x < y) else y)

#Get order item details with minimum subtotal for each order_id - reduceByKey
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items/")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), oi))
minSubtotalPerOrderId = orderItemsMap. \
reduceByKey(lambda x, y: 
  x if(float(x.split(",")[4]) < float(y.split(",")[4])) else y
  )
for i in minSubtotalPerOrderId.take(10): print(i)


Using aggregateByKey
aggregateByKey uses combiner
It is used when logic to compute intermediate values
 and logic to compute the final value using intermediate values are not the same
It is a bit tricky to implement
It takes 3 arguments
Initialize value – driven by output value type
Combine function or seqOp – 2 arguments
first argument – driven by output value type
the second argument – driven by input value type
Reduce function or combineOp – 2 arguments 
– driven by output value type

#Get revenue and count of items for each order id - aggregateByKey
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items/")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
for i in orderItemsMap.take(10): print(i)
revenuePerOrder = orderItemsMap. \
aggregateByKey((0.0, 0), 
  lambda x, y: (x[0] + y, x[1] + 1), 
  lambda x, y: (x[0] + y[0], x[1] + y[1]))
for i in revenuePerOrder.take(10): print(i) 
sortByKey:
Let us see how data can be sorted using Spark

Data can be sorted using sortByKey
Input RDD have to be paired RDD
Data can be sorted in ascending or descending order
By default, data will be sorted in natural ascending order of the key
We can pass False to sortByKey function to sort in descending order
Simple Sorting
Here are the steps for simple sorting

Understand data and identify the key and it’s data type using which data need to be sorted
Perform all the necessary transformations required before sorting the data
As part of last transformation before sorting the data, make sure the output have the sort key and value in the form of Paired RDD
Perform sortByKey to sort the data based on the key
Typically sortByKey will be followed by map to discard the key, if we need only value for saving or for further processing of data.

#Sort data by product price - sortByKey
products = sc.textFile("file:///home/ubuntu/data/retail_db/products")
productsMap = products. \
filter(lambda p: p.split(",")[4] != ""). \
map(lambda p: (float(p.split(",")[4]), p))
productsSortedByPrice = productsMap.sortByKey()
productsSortedMap = productsSortedByPrice. \
map(lambda p: p[1])

for i in productsSortedMap.take(10): print(i)


In case of composite sorting

We typically get key in the form of tuple having all the elements on which data need to be sorted
We can also accomplish sorting in ascending order of one element of composite key and sorting in descending order on another element (e.g.: ascending order by category and then descending order by product price) in some cases

#Sort data by product category and then product price descending - sortByKey
products = sc.textFile("file:///home/ubuntu/data/retail_db/products")
for i in products.take(10): print(i)
productsMap = products. \
filter(lambda p: p.split(",")[4] != ""). \
map(lambda p: ((int(p.split(",")[1]), -float(p.split(",")[4])), p))
for i in productsMap. \
sortByKey(). \
map(lambda p: p[1]). \
take(50): print(i)


set operations using Spark APIs.
a b
[10,20,30]
[20,40,50]
[20]
union
intersection
distinct
minus – subtract
When a union is performed, data will not be unique
Typically we have to use distinct after union to eliminate duplicate

Set operations - Prepare data - subsets of products for 2013-12 and 2014-01
orders = sc.textFile("file:///home/ubuntu/data/retail_db/orders")
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items")

orders201312 = orders. \
filter(lambda o: o.split(",")[1][:7] == "2013-12"). \
map(lambda o: (int(o.split(",")[0]), o))

orders201401 = orders. \
filter(lambda o: o.split(",")[1][:7] == "2014-01"). \
map(lambda o: (int(o.split(",")[0]), o))

orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), oi))

orderItems201312 = orders201312. \
join(orderItemsMap). \
map(lambda oi: oi[1][1])
orderItems201401 = orders201401. \
join(orderItemsMap). \
map(lambda oi: oi[1][1])
products201312 = orderItems201312. \
map(lambda p: int(p.split(",")[2]))
products201401 = orderItems201401. \
map(lambda p: int(p.split(",")[2]))

allproducts = products201312. \
union(products201401). \
distinct()

products201312 = orderItems201312. \
map(lambda p: int(p.split(",")[2]))
products201401 = orderItems201401. \
map(lambda p: int(p.split(",")[2]))

commonproducts = products201312.intersection(products201401)

#Saving as text files with delimiters - revenue per order id
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

from operator import add
revenuePerOrderId = orderItemsMap. \
reduceByKey(add). \
map(lambda r: str(r[0]) + "\t" + str(r[1]))

revenuePerOrderId. \
saveAsTextFile("/shiva_hdfs/mohan/revenue_per_order_id1")

# hadoop fs -ls /shiva_hdfs/mohan/revenue_per_order_id
# hadoop fs -tail /user/shiva/revenue_per_order_id/part-00000

for i in sc. \
textFile("/shiva_hdfs/mohan/revenue_per_order_id"). \
take(100):
  print(i)
  
  
  # Saving as text file - compression
# Get compression codec from /etc/hadoop/conf/core-site.xml
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

from operator import add
revenuePerOrderId = orderItemsMap. \
reduceByKey(add). \
map(lambda r: str(r[0]) + "\t" + str(r[1]))

revenuePerOrderId. \
saveAsTextFile("/shiva_hdfs/mohan/revenue_per_order_compressed",
  compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")
  
Save data in different file formats
Supported file formats
orc
json
parquet
avro (with databricks plugin)
Steps to save into different file formats
Make sure data is represented as Data Frame
Use write or save API to save Data Frame into different file formats
Use compression algorithm if required
# Saving as JSON - Get revenue per order id
orderItems = sc.textFile("file:///home/ubuntu/data/retail_db/order_items")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))


from operator import add
revenuePerOrderId = orderItemsMap. \
reduceByKey(add). \
map(lambda r: (r[0], round(r[1], 2)))

revenuePerOrderIdDF = revenuePerOrderId. \
toDF(schema=["order_id", "order_revenue"])
/shiva_hdfs/mohan/
revenuePerOrderIdDF.save("/shiva_hdfs/mohan/revenue_per_order_json", "json")
revenuePerOrderIdDF.write.json("/shiva_hdfs/mohan/revenue_per_order_json")

sqlContext.read.json("/shiva_hdfs/mohan/revenue_per_order_json").show()


Data Frames is nothing but RDD with structure.
we will know
what is Data Frames, how data frames can be created from (text) files, hive tables, relational databases using JDBC etc. We will also understand how data frame can be registered as in memory table/view and run SQL on top of it as well as some of the important functions that can be used to manipulate data as part of data frame operations.

Data Frames – Overview
Reading text data from files
Reading data from hive
Reading data from MySQL over JDBC
Data Frame Operations – Overview
Spark SQL – Overview
Functions to manipulate data

Data Frame can be created on any data set which have structure associated with it.
Attributes/columns in a data frame can be referred using names.
One can create data frame using data from files, hive tables, relational tables over JDBC.
Common functions on Data Frames

printSchema – to print the column names and data types of data frame
show – to preview data (default 20 records)
describe – to understand characteristics of data
count – to get number of records
collect – to convert data frame into Array
Once data frame is created, we can process data using 2 approaches.
Native Data Frame APIs

Register as temp table and run queries using spark.sql
To work with Data Frames as well as Spark SQL, we need to create object of type SparkSession


from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    master('local'). \
    appName('Create Dataframe over JDBC'). \
    getOrCreate()
	Let us see how we can read text data from files into data frame. spark.read also have APIs for other types of file formats.
	
We can use spark.read.csv or spark.read.text to read text data.
spark.read.csv can be used for comma separated data. Default field names will be in the form of _c0, _c1 etc

spark.read.text can be used to read fixed length data where there is no delimiter. Default field name is value.

We can define attribute names using toDF function
In either of the case data will be represented as strings
We can covert data types by using cast function – df.select(df.field.cast(IntegerType()))
We will see all other functions soon, but let us perform the task of reading the data into data frame and represent it in their original format.

orders = spark.read. \
		format('csv'). \
		load('file:///home/ubuntu/data/retail_db/order_items/')
		
from pyspark.sql.types import *
spark = SparkSession. \
  builder. \
  master('local'). \
  appName('CSV Example'). \
  getOrCreate()
fields = StructType([
    StructField("order_id",IntegerType(),True),
    StructField("order_date",StringType(),True),
    StructField("order_customer_id",IntegerType(),True),
	StructField("order_status",StringType(),True)])

finalSchema=StructType(fields)
orders = spark.read. \
  format('csv'). \
  schema(finalSchema). \
  load('file:///home/ubuntu/data/retail_db/order_items/')

orders.printSchema()
orders.show()

orderItems = spark.read. \
  format('csv'). \
  schema('''order_item_id StringType(), 
            order_item_order_id StringType(), 
            order_item_product_id StringType(), 
            order_item_quantity StringType(),
            order_item_subtotal StringType(),
            order_item_product_price StringType()
         '''). \
  load('file:///home/ubuntu/data/retail_db/order_items/')

orderItems.printSchema()
orderItems.show()

# In case you are using pycharm, first you need to create object of type SparkSession
spark = SparkSession. \
  builder. \
  master('local'). \
  appName('CSV Example'). \
  getOrCreate()

ordersCSV = spark.read. \
  csv('file:///home/ubuntu/data/retail_db/orders'). \
  toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

orderItemsCSV = spark.read. \
  csv('file:///home/ubuntu/data/retail_db/order_items'). \
  toDF('order_item_id', 'order_item_order_id', 'order_item_product_id', 
       'order_item_quantity', 'order_item_subtotal', 'order_item_product_price')

from pyspark.sql.types import IntegerType, FloatType

orders = ordersCSV. \
  withColumn('order_id', ordersCSV.order_id.cast(IntegerType())). \
  withColumn('order_customer_id', ordersCSV.order_customer_id.cast(IntegerType()))

orders.printSchema()
orders.show()

orderItems = orderItemsCSV.\
    withColumn('order_item_id', orderItemsCSV.order_item_id.cast(IntegerType())). \
    withColumn('order_item_order_id', orderItemsCSV.order_item_order_id.cast(IntegerType())). \
    withColumn('order_item_product_id', orderItemsCSV.order_item_product_id.cast(IntegerType())). \
    withColumn('order_item_quantity', orderItemsCSV.order_item_quantity.cast(IntegerType())). \
    withColumn('order_item_subtotal', orderItemsCSV.order_item_subtotal.cast(FloatType())). \
    withColumn('order_item_product_price', orderItemsCSV.order_item_product_price.cast(FloatType()))

orderItems.printSchema()
orderItems.show()

#Creates a spark data frame called as raw_data.
#JSON
dataframe = spark.read.json('file:///home/ubuntu/data/retail_db_json/orders/part-r-00000-990f5773-9005-49ba-b670-631286032674')
#TXT FILES# 
dataframe_txt = spark.read.text('text_data.txt')
#CSV FILES# 
dataframe_csv = spark.read.csv('csv_data.csv')
#PARQUET FILES# 
dataframe_parquet = spark.read.load('parquet_data.parquet')

Duplicate Values
dataframe_dropdup = df.dropDuplicates() dataframe_dropdup.show(10)
dataframe_dropdup.show(10)

“Select” Operation
#Show all entries in order_Status column
df.select("order_Status").show(10)
#Show all entries in title, author, rank, price columns
df.select("order_customer_id", "order_date", "order_id", "order_Status").show(10)

 “When” Operation
 # Show order_Status and assign
df.select("order_Status",when(df.order_status != 'CLOSED', 1).otherwise(0))

dataframe.filter(dataframe["order_Status"] == 'CLOSED').show(5)


# Show rows with specified order_status if in the given options
df [df.order_status.isin("CLOSED", 
"COMPLETE")].show(5)

“Like” Operation

# Show author and order_status is TRUE if order_status has " LOS " word in titles
df.select("order_date", "order_status",df.order_status.like("%LOS %")).show(15)

 “Startswith” — “ Endswith”
 df.select("order_date", "order_status", df.order_status.startswith("CLO")).show(5)
df.select("order_date", "order_status", df.order_status.endswith("ED")).show(5)

“Substring” Operation
df.select(df.order_status.substr(1, 3).alias("order_status")).show(5)
dataframe.select(dataframe.order_id.substr(3, 6).alias("order_status")).show(5)
dataframe.select(dataframe.order_id.substr(1, 6).alias("order_status")).show(5)

Add, Update & Remove Columns

Adding Columns

# Lit() is required while we are creating columns with exact values.
df = df.withColumn('new_column', 
F.lit('This is a new column'))
display(dataframe)

Updating Columns

# Update column 'amazon_product_url' with 'URL'
df = df.withColumnRenamed('new_column', 'URL')
dataframe.show(5)

Removing Columns

dataframe_remove = df.drop("URL")
dataframe_remove2 = dataframe \ .drop(dataframe.publisher).drop(dataframe.published_date).show(5)

Inspect Data

# Returns dataframe column names and data types
dataframe.dtypes
# Displays the content of dataframe
dataframe.show()
# Return first n rows
dataframe.head()
# Returns first row
dataframe.first()
# Return first n rows
dataframe.take(5)
# Computes summary statistics
df.describe().show()
# Returns columns of dataframe
df.columns
# Counts the number of rows in dataframe
dataframe.count()
# Counts the number of distinct rows in dataframe
dataframe.distinct().count()
# Prints plans including physical and logical
dataframe.explain(4)


“GroupBy” Operation

# Group by author, count the books of the authors in the groups
df.groupBy("order_status").count().show(10)

“Filter” Operation

# Filtering entries of title
# Only keeps records having value 'THE HOST'
df.filter(df["order_status"] == 'CLOSED').show(5)

from pyspark.sql import functions as F
df.where(F.isnull(F.col("order_status"))).show()
or
df.where(F.col("order_status").isNull()).show()
Missing & Replacing Values
df.where(F.col("customer_street").isNull()).show()
customer_street

# Replacing null values
dataframe.na.fill()
dataFrame.fillna()
dataFrameNaFunctions.fill()
# Returning new dataframe restricting rows with null valuesdataframe.na.drop()
dataFrame.dropna()
dataFrameNaFunctions.drop()
# Return new dataframe replacing one value with another
dataframe.na.replace(5, 15)
dataFrame.replace()
dataFrameNaFunctions.replace()

Repartitioning

It is possible to increase or decrease the existing level of partitioning in RDD Increasing can be actualized by using repartition(self, numPartitions) function which results in a new RDD that obtains same /higher number of partitions. Decreasing can be processed with coalesce(self, numPartitions, shuffle=False) function that results in new RDD with a reduced number of partitions to a specified number.

# Dataframe with 10 partitions
dataframe.repartition(10).rdd.getNumPartitions()
# Dataframe with 1 partition
dataframe.coalesce(1).rdd.getNumPartitions()

Running SQL Queries Programmatically

# Registering a table
dataframe.registerTempTable("df")
sc.sql("select * from df").show(3)
sc.sql("select \               
CASE WHEN description LIKE '%love%' THEN 'Love_Theme' \               WHEN description LIKE '%hate%' THEN 'Hate_Theme' \               WHEN description LIKE '%happy%' THEN 'Happiness_Theme' \               WHEN description LIKE '%anger%' THEN 'Anger_Theme' \               WHEN description LIKE '%horror%' THEN 'Horror_Theme' \               WHEN description LIKE '%death%' THEN 'Criminal_Theme' \               WHEN description LIKE '%detective%' THEN 'Mystery_Theme' \               ELSE 'Other_Themes' \               END Themes \       
from df").groupBy('Themes').count().show()


Output

Data Structures

DataFrame API uses RDD as a base and it converts SQL queries into low-level RDD functions. By using .rdd operation, a dataframe can be converted into RDD. It is also possible to convert Spark Dataframe into a string of RDD and Pandas formats.

# Converting dataframe into an RDD
rdd_convert = dataframe.rdd
# Converting dataframe into a RDD of string 
dataframe.toJSON().first()
# Obtaining contents of df as Pandas 
dataFramedataframe.toPandas()

Write & Save to Files

# Write & Save File in .parquet format
dataframe.select("author", "title", "rank", "description") \
.write \
.save("Rankings_Descriptions.parquet")

Stopping SparkSession
# End Spark Session
sc.stop()

If Hive and Spark 
are integrated, we can create data frames from data in Hive tables or run Spark SQL queries against it.

We can use spark.read.table to read data from Hive tables into Data Frame
We can prefix database name to table name while reading Hive tables into Data Frame
We can also run Hive queries directly using spark.sql
Both spark.read.table and spark.sql returns Data Frame

Let us see overview about Data Frame Operations. It is one of the 2 ways we can process Data Frames.

Selection or Projection – select
Filtering data – filter or where
Joins – join (supports outer join as well)
Aggregations – groupBy and agg with support of functions such as sum, avg, min, max etc
Sorting – sort or orderBy
Analytics Functions – aggregations, ranking and windowing functions


let us quickly look into some of the functions available in Data Frames.

Main package for functions pyspark.sql.functions
We can import by saying from pyspark.sql import functions as sf
You will see many functions which are similar to the functions in traditional databases.
These can be categorized into
String manipulation
Date manipulation
Type casting
Expressions such as case when
We will see some of the functions in action
substring
lower, upper
trim
date_format
trunc
Type Casting
case when

There are different interfaces to run Spark SQL queries

Using Hive
spark-sql
pyspark
Launching spark-sql
To launch the spark-sql use below command
spark-sql --master yarn --conf spark.ui.port=12567

To list the database use this command show databases;

We can use Hive DDL and DML statements to create tables and get data into tables. We need to understand

Create table command
Columns and Data Types
Row delimiter for text file format
Other file formats
Loading or Inserting data into Hive tables
Creating tables and loading data
Create database
Create tables using text file format
Make sure structure of data and the delimiters specified while create tables are same
If so, one can load data into Hive table using load command

create database shiva_retail_db_txt;
use shiva_retail_db_txt;

create table orders (
  order_id int,
  order_date string,
  order_customer_id int,
  order_status string
) row format delimited fields terminated by ','
stored as textfile;

load data local inpath '/data/retail_db/orders' into table orders;

create table order_items (
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quantity int,
  order_item_subtotal float,
  order_item_product_price float
) row format delimited fields terminated by ','
stored as textfile;

load data local inpath '/data/retail_db/order_items' into table order_items;


Using PySpark
All hive commands or queries can be submitted or executed using SQLContext’s sql API as part of PySpark

Running Hive Queries
Filtering (horizontal and vertical)
Functions
Row-level transformations
Joins
Aggregation
Sorting
Set Operations
Analytical Functions
Windowing Functions

Spark SQL or Hive have bunch of functions to apply transformations on the data.

String manipulating functions
Data manipulating functions
Typecast functions
Functions to deal with nulls
Aggregating functions
Conditional expressions
and more

String Manipulation
Understanding string manipulation APIs helps us processing data as part of lambda or anonymous functions used in Spark APIs

Extracting data – split and get required fields
Converting data types – type cast functions
Discarding unnecessary columns
Derive new expressions with data from different fields
create table customers (
customer_id int,
customer_fname varchar(45),
customer_lname varchar(45),
customer_email varchar(45),
customer_password varchar(45),
customer_street varchar(255),
customer_city varchar(45),
customer_state varchar(45),
customer_zipcode varchar(45)
) row format delimited 
fields terminated by ',' 
LINES TERMINATED BY '\n'
stored as textfile;
load data local inpath '/home/ubuntu/data/retail_db/customers/part-00000' into table customers;

As part of this topic, we will see important date functions in Spark SQL

Date Functions
current_date
current_timestamp
date_add
date_format
date_sub
datediff
day
dayofmonth
to_date
to_unix_timestamp
to_utc_timestamp
from_unixtime
from_utc_timestamp
minute
month
months_between
next_day

As part of this topic, we will talk about Aggregations very briefly.

Some of the important aggregate functions are count, average, min, max, sum etc.
To get number of records in a table we can use select count(1) from orders;
To get the sum of order items we can use the query select sum(order_item_subtoatl) from order_items;
Aggregate functions take multiple records as input and return one record as output.

As part of this topic, we will be talking about a very important function called CASE and also nvl

A CASE expression returns a value from the THEN portion of the clause.
To view the syntax of case use describe function case;

select order_status,
       case  
            when order_status IN ('CLOSED', 'COMPLETE') then 'No Action' 
            when order_status IN ('ON_HOLD', 'PAYMENT_REVIEW', 'PENDING', 'PENDING_PAYMENT', 'PROCESSING') then 'Pending Action'
            else 'Risky'
       end from orders limit 10;
	   
	   
	   Spark supports all possible join operations

Inner join
Outer join
Here are the steps involved in using join operations

Read data from files related to 2 datasets and convert into RDD
Apply map to transform each data set into paired RDD
Perform join using the paired RDDs
Apply further transformations using relevant APIs
Joins – Introduction
Spark provides several APIs to facilitate joins

join
leftOuterJoin
rightOuterJoin
fullOuterJoin
Typically joins are performed between 2 data sets with one too many relationships between them.

Inner Join
Following are the steps to perform join between 2 datasets

Read both the data sets
Apply map to convert datasets into paired RDDs
Perform join on the paired RDDs
Outer Join
We can perform

Left outer join using leftOuterJoin
Right outer join using rightOuterJoin
Full outer join using fullOuterJoin
Full outer join is nothing but the union of left outer join and right outer join on the same data sets.

select o.*, c.* from customers c left outer join orders o
on o.order_customer_id = c.customer_id
limit 10;

select count(1) from orders o inner join customers c
on o.order_customer_id = c.customer_id;

select count(1) from customers c left outer join orders o
on o.order_customer_id = c.customer_id;

select c.* from customers c left outer join orders o
on o.order_customer_id = c.customer_id
where o.order_customer_id is null;

The GROUP BY clause is used to group all the records in a result set using a particular collection column. It is used to query a group of records.
select o.order_id, o.order_date, o.order_status, round(sum(oi.order_item_subtotal), 2) order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 1000;


select o.order_id, o.order_date, o.order_status, round(sum(oi.order_item_subtotal), 2) order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 1000
order by o.order_date, order_revenue desc;

select o.order_id, o.order_date, o.order_status, round(sum(oi.order_item_subtotal), 2) order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 1000
distribute by o.order_date sort by o.order_date, order_revenue desc;
64999206
cc175----
-----------
input="/filelocation/path/test.csv"
df=spark.read.option("header","true").option("inferSchema","true").csv(input)
df.show()
df.printSchema()
df.createOrReplaceTempView("product_tbl")
df1=spark.sql("select concat(pname, '|', price) as mix from product_tbl where price>1000 order by price")
df1.write.option("compression","zlib").format("orc").save("/filelicoation/tables/output/products")
dbutils.fs.ls("/filelicoation/tables/output/products")

how to get distinct values from the column in spark df and load into list
list_yrs=[]
list_yrs=list(df.select('year').distinct().toPandas()['year'])
how to check whether the folder exist in the hdfs in the python script?
import subprocess as sp
dateset_name="RDATA_STANDARAD_RESULTS"
path="/lake/hills/dataset/%s/data"%dataset_name.lower()
command="hdfs dfs -test -d %spath
process=sp.Popen(command,stdout=sp.PIPE,stderr=sp.PIPE,shell=True)
stdout,stderr=process.communicate()
print(process.returncode,stdout,stderr)

how to convert bigint to timestamp in hive
spark.sql("select timestamp(from_unixtime(1563853753,'yyyy-MM-dd HH:mm:ss')) as ts").show()

