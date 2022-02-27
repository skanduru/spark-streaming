from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, FloatType, StringType, BooleanType, ArrayType, DateType

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
# coming from redis-server, it is of the form:
# {"customerId":"697986075","customerName":"John Phillips","reservationId":"1645877568114","amount":308.9}  
customerMessageSchema = StructType (
    [
        StructField("customerId", StringType()),
        StructField("customerName", StringType()),
        StructField("reservationId", StringType()),
        StructField("amount", FloatType())
    ]
)

# Schema for the second message is a reservation Message of the truck for the customer. It is of the form:
# {"reservationId":"1645877578197","customerId":"273330770","customerName":"Liz Habschied","truckNumber":"3317","reservationDate":"2022-02-26T12:12:58.197Z","checkInStatus":"CheckedOut","origin":"Wisconsin","destination":"Wisconsin"}

reservationMessageSchema = StructType (
    [
        StructField("reservationId", StringType()),
        StructField("customerId", StringType()),
        StructField("customerName", StringType()),
        StructField("truckNumber", StringType()),
        StructField("reservationDate", StringType()),
        StructField("checkInStatus", StringType()),
        StructField("origin", StringType()),
        StructField("destination", StringType()),
    ]
)
# 

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
 Received input messages:
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

And we are only concerned with the "key" records, ignoring the rest...
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
# execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedMessage
encodedEventStreamingDF = spark.sql(
    "select key, zSetEntries[0].element as eventMessage from RedisSortedSet")

"""
+----------------+--------------------+
|             key|        eventMessage|
+----------------+--------------------+
|            null|                null|
|    UGF5bWVudA==|                null|
|UmVzZXJ2YXRpb24=|                null|
|        VXNlcg==|                null|
|    Q3VzdG9tZXI=|                null|
|            null|                null|
|    UGF5bWVudA==|eyJjdXN0b21lcklkI...|
|UmVzZXJ2YXRpb24=|eyJyZXNlcnZhdGlvb...|
|    UGF5bWVudA==|eyJjdXN0b21lcklkI...|
|UmVzZXJ2YXRpb24=|eyJyZXNlcnZhdGlvb...|
|    UGF5bWVudA==|eyJjdXN0b21lcklkI...|
+----------------+--------------------+
"""


# base64 decode redisEvents: into customerMessage and reservationMessage
customerEventJSONifiedStreamDF = encodedEventStreamingDF.withColumn("eventMessage", unbase64(encodedEventStreamingDF.eventMessage).cast("string"))
reservationEventJSONifiedStreamDF = encodedEventStreamingDF.withColumn("eventMessage", unbase64(encodedEventStreamingDF.eventMessage).cast("string"))

# Filter out the messages based on its unique fields into payment and reservation records
customerStreamDF = customerEventJSONifiedStreamDF.filter(~col("eventMessage").contains("truckNumber"))
reservationStreamDF = reservationEventJSONifiedStreamDF.filter(col("eventMessage").contains("truckNumber"))


# Now parse the JSON in the Customer record and store in a temporary view called CustomerRecords

customerStreamDF  \
    .withColumn("eventMessage",  \
    from_json("eventMessage", customerMessageSchema)) \
    .select(col('eventMessage.*')) \
    .createOrReplaceTempView('customerRecords')

# ANd parse the JSON in the reservation records store in a temporary view called reservationRecords

reservationStreamDF  \
    .withColumn("eventMessage",  \
    from_json("eventMessage", reservationMessageSchema)) \
    .select(col('eventMessage.*')) \
    .createOrReplaceTempView('reservationRecords')


# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
customerRecordsDF = spark.sql(
    "select * from customerRecords "
)


reservationRecordsDF = spark.sql(
    "select customerId as accountId, reservationId, customerName, truckNumber, reservationDate, checkInStatus, origin, destination from reservationRecords"
)
"""
reservationRecordsDF = spark.sql(
    "select customerId as accountId, truckNumber, origin, destination from reservationRecords"
)
"""

# reservationRecordsDF.writeStream.outputMode("append").format("console").option("truncate", False).start().awaitTermination()
# reservationRecordsDF.writeStream.outputMode("append").format("console").option("truncate", False).start().awaitTermination()
"""
+----------+--------------+-------------+------+
|customerId|customerName  |reservationId|amount|
+----------+--------------+-------------+------+
|697986075 |John Phillips |1645877568114|308.9 |
|486712698 |Manoj Phillips|1645877568117|778.82|
|553120617 |Bobby Jones   |1645877568113|722.2 |
|788935702 |Liz Wu        |1645877568124|909.25|
|953701291 |Gail Fibonnaci|1645877568132|952.7 |
|7001036   |Jaya Anderson |1645877568119|567.9 |
|793829559 |Sean Jones    |1645877568117|438.84|
|127200206 |Sean Ahmed    |1645877590210|710.38|
|428115213 |Ben Hansen    |1645877568100|606.67|
|793829559 |Sean Jones    |1645877568117|886.65|
|526181430 |David Anandh  |1645877568113|230.54|
+----------+--------------+-------------+------+

+-------------+----------+----------------+-----------+------------------------
|reservationId|customerId|customerName    |truckNumber|reservationDate         |checkInStatus|origin      |destination |
+-------------+------------+------------+
|1645877578197|273330770 |Liz Habschied   |3317       |2022-02-26T12:12:58.197Z|CheckedOut   |Wisconsin   |Wisconsin   |
|1645877580198|634834451 |Senthil Phillips|9616       |2022-02-26T12:13:00.198Z|CheckedOut   |Canada      |Texas       |
+-------------+----------+----------------+-----------+------------------------+-------------+------------+------------+


"""


# join the streaming dataframes on the customerId to get all info in the same dataframe
customerReservationDF = customerRecordsDF.join(reservationRecordsDF, expr ("""
   customerId = accountId
"""
))

# sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application 
#
# customerReservationDF.writeStream.outputMode("append").format("console").option("truncate", False).start().awaitTermination()
customerReservationDF.selectExpr("cast(customerId as string) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "checkin-status") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()

"""
{"customerId":"560546029","customerName":"Craig Fibonnaci","reservationId":"1645930200883","amount":43.89,"accountId":"560546029","reservationId":"1645929824556","customerName":"Craig Fibonnaci","truckNumber":"8543","reservationDate":"2022-02-27T02:43:44.556Z","checkInStatus":"CheckedOut","origin":"Georgia","destination":"Arizona"}
"""
