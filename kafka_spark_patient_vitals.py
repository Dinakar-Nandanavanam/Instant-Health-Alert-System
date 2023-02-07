
# Import dependent libraries
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import from_json


# Initiate Spark Session
spark = SparkSession  \
        .builder \
        .master("local") \
        .appName("VitalInfoConsumer")  \
        .enableHiveSupport() \
        .getOrCreate()
        
spark.sparkContext.setLogLevel('ERROR')


# Set bootstrap server and kafka-topic of stream source
kafka_bootstrap_server = "localhost:9092"
kafka_topic = "Patients-Vital-Info"

storage_path = "health-alert/patients-vital-info/"
checkpoint_path = "health-alert/cp-vital-info/"
offset_mode = "earliest"

# Read innput raw streams of Patients Vital Info
vitalInfoRawStream = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server)  \
        .option("subscribe", kafka_topic)  \
        .option("startingOffsets", offset_mode)  \
        .load()


# Define Vital Info Schema
vitalInfoSchema = StructType() \
        .add("customerId", IntegerType()) \
        .add("heartBeat", IntegerType()) \
	    .add("bp",IntegerType()) 
        
        
# Generate Structured Vital Info stream from input raw stream
vitalInfoDF = vitalInfoRawStream.select(from_json(col("value").cast("string"), vitalInfoSchema).alias("health_df")).select("health_df.*")


# Generate Vital Info stream with derived timestamp column
vitalInfoDFExt = vitalInfoDF \
       .withColumn("message_time",current_timestamp())	
       


# Console Output of Vital Info stream
vitalInfoStreamConsole = vitalInfoDFExt \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="10 seconds") \
       .start()
       

# Save Vital Info stream to hdfs storage in parquet format
vitalInfoStreamStorage = vitalInfoDFExt \
       .writeStream \
       .outputMode("append") \
       .format("parquet") \
       .option("truncate", "false") \
       .option("path", storage_path) \
       .option("checkpointLocation", checkpoint_path) \
       .trigger(processingTime="10 seconds") \
       .start()
 

# Terminations of streams
vitalInfoStreamConsole.awaitTermination()

vitalInfoStreamStorage.awaitTermination()
