from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaToParquet").getOrCreate()

kafka_bootstrap_server = "kafka:9092"
kafka_topics = "auth_events,listen_events,page_view_events,status_change_events"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
    .option("subscribe", kafka_topics) \
    .option("startingOffsets", "earliest") \
    .load()

kafka_df = df.selectExpr("CAST(key AS STRING) as key", 
                         "CAST(value AS STRING) as value", 
                         "topic")

query = kafka_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/parquet_output") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoint_dir") \
    .partitionBy("topic") \
    .trigger(processingTime='60 seconds') \
    .start()


# Keep running indefinitely
query.awaitTermination()
