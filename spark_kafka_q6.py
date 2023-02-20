from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode, split, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType,DoubleType
import json

kafka_brokers=""
kafka_src_topic_name=""
kafka_dest_topic_name1=""

def writeToSinks(stream_df, epochId):
    global kafka_brokers
    global kafka_src_topic_name
    global kafka_dest_topic_name1
    # print("epochId:"+ epochId)
    # stream_df.cache()
    # .option("kafka.bootstrap.servers", "localhost:9092") \
    # .option("topic", "pxljson") \
    print("kafka_brokers:::" + kafka_brokers)
    print("kafka_src_topic_name:::" + kafka_src_topic_name)
    print("kafka_dest_topic_name1:::" + kafka_dest_topic_name1)

    stream_df.selectExpr("vechicleid AS key", "to_json(struct(*)) AS value")\
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("topic", kafka_dest_topic_name1) \
    .option("checkpointLocation", "checkpoints") \
    .save()
    stream_df.write.format("console").save()
    # stream_df.uncache

if __name__ == "__main__":
    # global kafka_brokers
    # global kafka_src_topic_name
    # global kafka_dest_topic_name1
    propertiesFile = open('properties.json')
    properties = json.load(propertiesFile)

    kafka_brokers = properties["Kafka_brokers"]  # "test"
    kafka_src_topic_name = properties["Kafka_source_topic"]  # "test"
    kafka_dest_topic_name1 = properties["Kafka_destination_topic1"]

    spark = SparkSession \
        .builder \
        .appName("wordCounter").getOrCreate()
        
        
    file1 = "DriverProfiles.csv"
    schema1 = "driverId STRING, name STRING,address STRING, city STRING"
    static_df = (spark.read.format("csv") \
        .option("header", "true") \
        .schema(schema1) \
        .option("mode", "FAILFAST") \
        .option("nullValue", "") \
        .load(file1))
    print("Reading fron file")
    static_df.show(10)

    # .option("kafka.bootstrap.servers", "localhost:9092") \
    # .option("subscribe", "test") \
    # Read the data from kafka
    stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", kafka_src_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Print out the dataframa schema
    print("Input Data schema")
    stream_df.printSchema()
    
    
    
    # Convert the datatype for value to a string
    #string_df = stream_df.selectExpr("CAST(value AS STRING)")
    string_df = stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    # Print out the new dataframa schema
    print("Input Data frame schema")
    string_df.printSchema()
    
    # Create a schema for the stream_df
    schema = StructType([
        StructField("vechicleid", StringType()),
        StructField("driverId", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("speed", DoubleType())
        ])
    
    # Select the data present in the column value and apply the schema on it
    json_df = string_df.withColumn("jsonData", from_json(col("value"), schema)).select("jsondata.*")    
    
    # Print out the dataframa schema
    print("schema")
    json_df.printSchema()
    
    
    df3=json_df.join(static_df,"driverId")
    print("join schema")
    df3.printSchema()
    
    #streaming_df = json_df.select("json.*")
    join_df = json_df.join(static_df,"driverId")


    join_selectdf=join_df.select("*").where("speed > 50")
    
    
    # Write output to the terminal
    #json_df.writeStream.format("console").outputMode("append").start().awaitTermination()
    # join_selectdf.writeStream.format("console").outputMode("append").start().awaitTermination()

    # Write output to kafka topic
    # join_selectdf.selectExpr("vechicleid AS key", "to_json(struct(*)) AS value")\
    #         .writeStream\
    #         .format("kafka")\
    #         .outputMode("append")\
    #         .option("kafka.bootstrap.servers", "localhost:9092")\
    #         .option("topic", "pxljson")\
    #         .option("checkpointLocation", "checkpoints")\
    #         .start()\
    #         .awaitTermination()
    # join_selectdf.selectExpr("vechicleid AS key", "to_json(struct(*)) AS value") \
    join_selectdf.writeStream \
        .foreachBatch(writeToSinks) \
        .outputMode("append") \
        .start() \
        .awaitTermination()

