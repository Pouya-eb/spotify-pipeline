from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaToParquet").getOrCreate()

kafka_bootstrap_server = "kafka:9092"
kafka_topics = "auth_events,listen_events,page_view_events,status_change_events"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
    .option("subscribe", kafka_topics) \
    .option("includeHeaders", "true") \
    .option("startingOffsets", "earliest") \
    .load()

kafka_df = df.selectExpr("CAST(key AS STRING) as key", 
                         "CAST(value AS STRING) as value", 
                         "topic")

query = kafka_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/opt/spark-data/parquet_output") \
    .option("checkpointLocation", "/opt/spark-data/checkpoint_dir") \
    .partitionBy("topic") \
    .trigger(processingTime='60 seconds') \
    .start()

query.awaitTermination()