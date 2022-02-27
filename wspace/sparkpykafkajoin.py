from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr, lit
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

# create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic
# schema derived from the Note on The Redis Source for Kafka
redisMessageSchema = StructType(
     [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue", StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("score", StringType())
            ]))
        )
     ]
)

# create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
# from the JSON format for the cutomer defined below:
customerSchema = StructType (
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("birthDay", StringType())
    ]
)


# create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
# from the kafka topic stedi events
stediCustomerRiskSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("riskDate", StringType()),
    ]
)

#create a spark application object and set log level to WARN
spark = SparkSession.builder.appName("stedi-risk-data").getOrCreate()
spark.sparkContext.setLogLevel('WARN')


# using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
redisEventRawStreamingDF = spark                            \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092")    \
    .option("subscribe", "redis-server")                 \
    .option("startingOffsets", "earliest")               \
    .load()

# Here the kafka message is of the form :
#           'key': key, 'value': 'redisMessageSchema'
# cast the value column in the streaming dataframe as a STRING
redisEventStreamingDF = redisEventRawStreamingDF.selectExpr(
    "cast(key as string) rkey", "cast(value as string) redisEventJSON"
)
"""
 Received output:
+--------------------+--------------------+
|                rkey|      redisEventJSON|
+--------------------+--------------------+
|com.moilioncircle...|{"auxKey":"redis-...|
|com.moilioncircle...|{"auxKey":"redis-...|
|com.moilioncircle...|{"auxKey":"ctime"...|
|com.moilioncircle...|{"auxKey":"used-m...|
|com.moilioncircle...|{"auxKey":"repl-s...|
|com.moilioncircle...|{"auxKey":"repl-i...|
|com.moilioncircle...|{"auxKey":"repl-o...|
|com.moilioncircle...|{"auxKey":"aof-pr...|
|com.moilioncircle...|{"db":{"dbNumber"...|
|com.moilioncircle...|{"db":{"dbNumber"...|
|com.moilioncircle...|{"db":{"dbNumber"...|
|com.moilioncircle...|{"db":{"dbNumber"...|
|com.moilioncircle...|{"checksum":80292...|
|com.moilioncircle...|         {"index":0}|
|com.moilioncircle...|{"key":"UGF5bWVud...|
|com.moilioncircle...|{"key":"UmVzZXJ2Y...|
|com.moilioncircle...|{"key":"UGF5bWVud...|
|com.moilioncircle...|{"key":"UmVzZXJ2Y...|
|com.moilioncircle...|{"key":"UGF5bWVud...|
|com.moilioncircle...|{"key":"UmVzZXJ2Y...|
+--------------------+--------------------+

And we are only concerned about the "key" records, ignoring the rest...
"""

# TO-DO:; parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
#
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
redisEventStreamingDF.withColumn("redisEventJSON", from_json("redisEventJSON",
        redisMessageSchema)) \
        .select(col('redisEventJSON.*')) \
        .createOrReplaceTempView("RedisSortedSet")

#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet
encodedEventStreamingDF = spark.sql(
    "select zSetEntries[0].element as encodedCustomer from RedisSortedSet")

# execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer

# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
encodedCustomerRecordsDF = encodedEventStreamingDF.withColumn(
    "encodedCustomer", unbase64(encodedEventStreamingDF.encodedCustomer)
    .cast("string"))


# take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}

# parse the JSON in the Customer record and store in a temporary view called CustomerRecords

encodedCustomerRecordsDF  \
    .withColumn("encodedCustomer",  \
    from_json("encodedCustomer", customerSchema)) \
    .select(col('encodedCustomer.*')) \
    .createOrReplaceTempView('CustomerRecords')

# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = spark.sql(
    "select email, birthDay from CustomerRecords "
    "where email is not null and birthDay is not null")

# Split the birth year as a separate field from the birthday
# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.withColumn(
    "birthYear", split("CustomerRecords.birthDay", "-").getItem(0).alias("birthYear"))
emailAndBirthYearStreamingDF = emailAndBirthYearStreamingDF.drop("birthday")

# using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
stediEventRawStreamingDF = spark                            \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092")    \
    .option("subscribe", "stedi-events")                 \
    .option("startingOffsets", "earliest")               \
    .load()

# cast the value column in the streaming dataframe as a STRING
customerRiskStreamingDF = stediEventRawStreamingDF.selectExpr(
    "cast(key as string) rkey", "cast(value as string) stediEventJSON"
)

# parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
customerRiskStreamingDF.withColumn("stediEventJSON", from_json("stediEventJSON",
        stediCustomerRiskSchema)) \
        .select(col('stediEventJSON.*')) \
        .createOrReplaceTempView("CustomerRisk")

# execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskDF = spark.sql(
    "select customer, score from CustomerRisk")

# join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
do_join = False
if do_join:
    customerRiskProfileStreamingDF = customerRiskDF.join(emailAndBirthYearStreamingDF, expr ("""
       customer = email
    """
    ))
else:
    """
       XXX: There is an issue with STEDI application and no events are coming out.
            So we can't join them until the producer is fixed in this environ
    """
    customerRiskProfileStreamingDF = emailAndBirthYearStreamingDF.withColumn("score", lit("25.0").cast(StringType()))

# sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"}
customerRiskProfileStreamingDF.selectExpr("cast(email as string) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "risky-topic") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()
