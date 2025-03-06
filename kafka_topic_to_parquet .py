from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, lit

spark = SparkSession.builder.appName("SpotifyKafkaToParquet").getOrCreate()

kafka_bootstrap_servers = "kafka:9092"
kafka_topics = "auth_events,listen_events,page_view_events,status_change_events"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topics) \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as value", "topic", "timestamp") \
       .withColumn("date", date_format("timestamp", "yyyy-MM-dd"))

def process_batch(batch_df, batch_id):
    batch_df = batch_df.withColumn("batch_id", lit(batch_id))
    batch_df.write \
        .option("maxRecordsPerFile", 50000) \
        .partitionBy("topic", "date", "batch_id") \
        .mode("append") \
        .parquet("hdfs://namenode:9000/parquet_output")

query = df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoint_dir") \
    .trigger(processingTime="60 seconds") \
    .start()

query.awaitTermination()
