from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import from_unixtime, col, from_json
from pyspark.sql.types import LongType, DoubleType
import time

spark = SparkSession.builder \
    .appName("IncrementalParquetReader") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/parquet_output"
checkpoint_dir = "hdfs://namenode:9000/checkpoints"

fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

def get_checkpoint(topic):
    cp_file = f"{checkpoint_dir}/{topic}.checkpoint"
    path = spark._jvm.org.apache.hadoop.fs.Path(cp_file)
    try:
        if fs.exists(path):
            input_stream = fs.open(path)
            reader = spark._jvm.java.io.BufferedReader(
                spark._jvm.java.io.InputStreamReader(input_stream, "UTF-8"))
            line = reader.readLine()
            reader.close()
            input_stream.close()
            return line.strip() if line is not None else None
    except Exception as e:
        print(f"Error reading checkpoint for {topic}: {e}")
    return None

def update_checkpoint(topic, file_name):
    cp_file = f"{checkpoint_dir}/{topic}.checkpoint"
    path = spark._jvm.org.apache.hadoop.fs.Path(cp_file)
    try:
        output_stream = fs.create(path, True)
        output_stream.write(bytearray(file_name, "utf-8"))
        output_stream.close()
    except Exception as e:
        print(f"Error updating checkpoint for {topic}: {e}")

def flatten_json(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if str(field.dataType).startswith("StringType"):
            sample = df.select(field.name).filter(col(field.name).isNotNull()).first()
            if sample:
                try:
                    schema = spark.read.json(df.select(field.name).rdd.map(lambda r: r[0])).schema
                    df = df.withColumn(field.name, from_json(col(field.name), schema)) \
                           .select("*", f"{field.name}.*").drop(field.name)
                except Exception as e:
                    print(f"Skipping column {field.name}: {e}")
    return df

def safe_select(df: DataFrame, columns: list) -> DataFrame:
    available = [c for c in columns if c in df.columns]
    return df.select(*available)

def process_all_files():
    global spark_dataframes, final_checkpoints
    spark_dataframes = {}
    final_checkpoints = {}
    topics = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(base_path))
    topic_names = [t.getPath().getName() for t in topics if "topic=" in t.getPath().getName()]
    
    for topic in topic_names:
        topic_cleaned = topic.replace("topic=", "")
        topic_path = f"{base_path}/{topic}"
        print(f"Processing topic: {topic_cleaned}")
        last_checkpoint = get_checkpoint(topic_cleaned)
        print(f"Last checkpoint for {topic_cleaned}: {last_checkpoint}")
        
        date_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(topic_path))
        date_dirs = [d for d in date_status if "date=" in d.getPath().getName()]
        sorted_date_dirs = sorted(date_dirs, key=lambda d: d.getPath().getName())
        
        topic_processed = False
        for date_dir in sorted_date_dirs:
            date_name = date_dir.getPath().getName()
            date_path = f"{topic_path}/{date_name}"
            batch_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(date_path))
            batch_dirs = [b for b in batch_status if "batch_id=" in b.getPath().getName()]
            sorted_batch_dirs = sorted(batch_dirs, key=lambda b: b.getPath().getName())
            
            for batch_dir in sorted_batch_dirs:
                batch_name = batch_dir.getPath().getName()
                batch_path = f"{date_path}/{batch_name}"
                file_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(batch_path))
                parquet_files = [f.getPath().toString() for f in file_status 
                                 if f.getPath().getName().endswith(".parquet")]
                if not parquet_files:
                    print(f"No parquet files in {batch_path}")
                    continue
                sorted_parquet_files = sorted(parquet_files)
                for file in sorted_parquet_files:
                    if last_checkpoint is not None and file <= last_checkpoint:
                        continue
                    print(f"Reading Parquet File: {file}")
                    try:
                        df = spark.read.parquet(file)
                        df = flatten_json(df)
                    except Exception as e:
                        print(f"Error reading parquet file {file}: {e}")
                        continue
                    count = df.count()
                    print(f"Processed {count} rows from {file} (topic: {topic_cleaned})")
                    update_checkpoint(topic_cleaned, file)
                    last_checkpoint = file
                    spark_dataframes[topic_cleaned] = df
                    topic_processed = True
                    break
                if topic_processed:
                    break
            if topic_processed:
                break
        final_checkpoints[topic_cleaned] = last_checkpoint if last_checkpoint is not None else get_checkpoint(topic_cleaned)
    
    def finalize_checkpoints(cp_dict: dict):
        for topic, cp in cp_dict.items():
            if cp is not None:
                update_checkpoint(topic, cp)
                print(f"Final checkpoint for {topic}: {cp}")
            else:
                print(f"No checkpoint available for {topic}")
    finalize_checkpoints(final_checkpoints)
    
    for topic, df in spark_dataframes.items():
        globals()[f"{topic}"] = df
    print("Available DataFrames:")
    for topic in spark_dataframes.keys():
        print(topic)
    print("Waiting for next run...")

while True:
    try:
        process_all_files()
        
        # OPTIONAL: Process cleaning, conversion, and write dimensions/fact tables
        if 'auth_events' in globals():
            def clean_dataframe(df, prefix="value_"):
                for col_name in df.columns:
                    if col_name.startswith(prefix):
                        df = df.withColumnRenamed(col_name, col_name[len(prefix):])
                if 'key' in df.columns:
                    df = df.drop('key')
                return df
            auth_events = clean_dataframe(auth_events)
            if 'page_view_events' in globals():
                page_view_events = clean_dataframe(page_view_events)
            if 'status_change_events' in globals():
                status_change_events = clean_dataframe(status_change_events)
            if 'listen_events' in globals():
                listen_events = clean_dataframe(listen_events)
        
            def convert_unix_cols(df: DataFrame, unix_cols: list) -> DataFrame:
                for unix_col in unix_cols:
                    if unix_col in df.columns:
                        if isinstance(df.schema[unix_col].dataType, (LongType, DoubleType)):
                            df = df.withColumn(unix_col, from_unixtime((col(unix_col) / 1000)).cast("timestamp"))
                        else:
                            print(f"Skipping column '{unix_col}' as it's not numeric.")
                    else:
                        print(f"Column '{unix_col}' not found in DataFrame.")
                return df
        
            dfs_list = []
            for topic in ['auth_events', 'page_view_events', 'status_change_events', 'listen_events']:
                if topic in globals():
                    dfs_list.append(globals()[topic])
            unix_timestamp_cols = ['registration', 'ts']
            cleaned_dfs = [convert_unix_cols(df, unix_timestamp_cols) for df in dfs_list]
            if 'auth_events' in globals() and cleaned_dfs:
                auth_events = cleaned_dfs[0]
            if 'page_view_events' in globals() and len(cleaned_dfs) > 1:
                page_view_events = cleaned_dfs[1]
            if 'status_change_events' in globals() and len(cleaned_dfs) > 2:
                status_change_events = cleaned_dfs[2]
            if 'listen_events' in globals() and len(cleaned_dfs) > 3:
                listen_events = cleaned_dfs[3]
        
            if 'page_view_events' in globals():
                page_view_events = page_view_events.na.drop()
            if 'auth_events' in globals():
                auth_events = auth_events.na.drop()
        
            def safe_select(df: DataFrame, cols: list) -> DataFrame:
                available = [c for c in cols if c in df.columns]
                return df.select(*available)
        
            # Create and write out dimensions and fact tables if data exists
            if 'auth_events' in globals():
                dim_user = safe_select(auth_events, ["userId", "firstName", "lastName", "gender", "level", "registration"]) \
                           .dropDuplicates(["userId"])
                dim_location = safe_select(auth_events, ["city", "state", "zip", "lat", "lon"]).distinct()
                dim_session = safe_select(auth_events, ["sessionId", "userAgent", "userId"]).distinct()
        
                dim_user.write.mode('append').parquet('/data/dim_user')
                dim_location.write.mode('append').parquet('/data/dim_location')
                dim_session.write.mode('append').parquet('/data/dim_session')
        
            if 'listen_events' in globals():
                fact_listens = safe_select(listen_events, ["userId", "sessionId", "itemInSession", "song", "artist", "duration", "ts"])
                fact_listens.write.mode('append').parquet('/data/fact_listens')
        
            if 'page_view_events' in globals():
                fact_page_views = safe_select(page_view_events, ["userId", "sessionId", "page", "method", "status", "ts"])
                fact_page_views.write.mode('append').parquet('/data/fact_page_views')
        
            if 'status_change_events' in globals():
                fact_status_changes = safe_select(status_change_events, ["userId", "sessionId", "level", "ts"])
                fact_status_changes.write.mode('append').parquet('/data/fact_status_changes')
    
    except Exception as e:
        print(f"Error in processing loop: {e}")
    
    # Wait for 5 minutes before the next run
    time.sleep(300)
