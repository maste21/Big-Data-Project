from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, IntegerType, StringType
import os
import base64
import datetime


PROJECT_ROOT = r"D:\Big-Data-Project"
YOLOV5_DIR = os.path.join(PROJECT_ROOT, "yolov5")
LOCAL_IMAGES_DIR = os.path.join(PROJECT_ROOT, "images") 
LOCAL_RESULTS_DIR = os.path.join(LOCAL_IMAGES_DIR, "results") 

# Ensure local directories exist
os.makedirs(LOCAL_IMAGES_DIR, exist_ok=True)
os.makedirs(LOCAL_RESULTS_DIR, exist_ok=True)

# --- Configuration for Kafka and Elasticsearch ---
KAFKA_TOPIC_NAME_CONS = "hello"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

c = 0 

es_write_conf = {
    "es.nodes": "localhost",
    "es.port": "9200",
    "es.resource": 'practice1/docs',
    "es.input.json": "false",
    "es.net.http.auth.user": "elastic",
    "es.net.http.auth.pass": "hdCHd8oqLQRxqqbCAsFU",
  
    "es.nodes.wan.only": "true", 
    "es.net.ssl": "true", 
    "es.net.ssl.cert.allow.self.signed": "true", 
   
}

if __name__ == "__main__":
    print("Integrating Kafka, Spark And ES...Demo Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("StructuredNetwork") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    vid_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .option("enable.auto.commit", "true") \
        .load()

    print("Printing Schema of image_data: ")
    print(type(vid_detail_df))
    vid_detail_df.printSchema()

    detail_df1 = vid_detail_df.selectExpr("CAST(value AS STRING)")

    # Define the schema for the incoming Kafka message
    vid_detail_schema = StructType() \
        .add("camera_id", IntegerType()) \
        .add("date", StringType()) \
        .add("time", StringType()) \
        .add("rows", IntegerType()) \
        .add("cols", IntegerType()) \
        .add("data", StringType()) # Base64 encoded image data

    vid_detail_df2 = detail_df1 \
        .select(from_json(col("value"), vid_detail_schema).alias("video_detail"))

    vid_detail_df3 = vid_detail_df2.select("video_detail.data", "video_detail.date", "video_detail.time", "video_detail.camera_id")

    # Helper function 
    def getrows(df, rownums=None):
        if rownums is None:
            rownums = [0] # Default to first row
        return df.rdd.zipWithIndex().filter(lambda x: x[1] in rownums).map(lambda x: x[0])

    # Function to process each micro-batch
    def foreach_batch_function(df, epoch_id):
        #
        img_data_row = df.collect()

        if not img_data_row: 
            print(f"Epoch {epoch_id}: No data in batch.")
            return

        # Access the first row's data
        base64_string_from_kafka = img_data_row[0][0] 
        kafka_date = img_data_row[0][1]
        kafka_time = img_data_row[0][2]
        kafka_camera_id = img_data_row[0][3]

        global c 
        global es_write_conf 

        try:
            base64_string_raw = base64_string_from_kafka
            if base64_string_from_kafka.startswith("b'") and base64_string_from_kafka.endswith("'"):
                base64_string_raw = base64_string_from_kafka[2:-1] 

            decoded_image_bytes = base64.b64decode(base64_string_raw)
        except Exception as e:
            print(f"Error decoding base64 image for epoch {epoch_id}: {e}")
            return 

        local_raw_image_path = os.path.join(LOCAL_IMAGES_DIR, f"img_{c}.jpg")
        with open(local_raw_image_path, 'wb') as f:
            f.write(decoded_image_bytes)
        print(f"Epoch {epoch_id}: Raw image saved to {local_raw_image_path}")

        
        yolov5_detect_script = os.path.join(YOLOV5_DIR, "detect.py")
        
        yolov5_command = (
            f"python {yolov5_detect_script} "
            f"--source \"{local_raw_image_path}\" "
            f"--project \"{LOCAL_RESULTS_DIR}\" "
            f"--name \"run_{c}\" "
            f"--exist-ok"
        )
        print(f"Epoch {epoch_id}: Running YOLOv5 command: {yolov5_command}")
        os.system(yolov5_command)
        print(f"Epoch {epoch_id}: YOLOv5 processing for img_{c}.jpg completed.")

       
        yolov5_processed_image_relative_web_path = f"/images/results/run_{c}/img_{c}.jpg"
        full_web_url_to_image = yolov5_processed_image_relative_web_path.replace(os.sep, '/') 
        es_data = {
            'data': base64_string_from_kafka, 
            'val': full_web_url_to_image, 
            'date': kafka_date,
            'time': kafka_time,
            'camera_id': kafka_camera_id
        }

        rdd = spark.sparkContext.parallelize([es_data])
        new_rdd = rdd.map(lambda x: ('key', x))

        
        try:
            new_rdd.saveAsNewAPIHadoopFile(
                path='_',
                outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                keyClass="org.apache.hadoop.io.NullWritable",
                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                conf=es_write_conf
            )
            print(f"Epoch {epoch_id}: Data for img_{c}.jpg pushed to Elasticsearch successfully.")
        except Exception as e:
            print(f"Epoch {epoch_id}: Error pushing data to Elasticsearch for img_{c}.jpg: {e}")

        c += 1

    vid_detail_write_stream = vid_detail_df3.writeStream.foreachBatch(foreach_batch_function).start()

    vid_detail_write_stream.awaitTermination()

    print("PySpark Structured Streaming with Kafka Demo Application Completed....")