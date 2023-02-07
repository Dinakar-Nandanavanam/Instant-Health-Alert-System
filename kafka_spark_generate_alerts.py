
# Import dependent libraries
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, to_json
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql import HiveContext
import json



# Initiate Spark Session
spark = SparkSession \
        .builder \
        .master("local") \
        .appName("HealthAlerts") \
        .enableHiveSupport() \
        .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')


# Set bootstrap server and kafka-topic of stream source
kafka_bootstrap_server = "localhost:9092"
kafka_topic = "Health-Alert-Messages"
checkpoint_path = "health-alert/cp-alert-message/"

stream_src_path = "health-alert/patients-vital-info/"


# generated contact dataframe from hive table
contact_df=spark.sql("select * from healthdb.patients_contact_info")

# generate reference dataframe from hbase on hive tive
reference_df=spark.sql("select * from healthdb.threshold_ref_hive")


# Define Vital Info Schema with timestamp
vitalInfoSchema = StructType() \
        .add("customerId", IntegerType()) \
        .add("heartBeat", IntegerType()) \
	    .add("bp", IntegerType()) \
        .add("message_time", TimestampType())
        

# Read innput streams of Patients Vital Info from hdfs storage
vital_info_df = spark \
	.readStream \
	.format("parquet") \
    .option("maxFilesPerTrigger","1") \
	.schema(vitalInfoSchema) \
	.load(stream_src_path)
    

# Generate Structured Patient Details stream from input patient details stream joing with contact dataframe
patient_details_df = vital_info_df.join(contact_df, vital_info_df.customerId == contact_df.patientid)


# register patient details dataframe to spark temp table 
patient_details_df.registerTempTable("patient_details_tbl")

# register reference dataframe to spark temp table 
reference_df.registerTempTable("reference_tbl")


# function to get query for alert dataframe
def get_df_query(reading_type):
    query = f"select a.patientname,a.age,a.patientaddress,a.phone_number,a.admitted_ward, \
    a.bp,a.heartBeat,a.message_time as input_message_time,b.alert_message \
    from patient_details_tbl a, reference_tbl b \
    where b.attribute = '{reading_type}' \
    and (a.age>=b.low_age_limit and a.age<=b.high_age_limit) \
    and (a.{reading_type}>=b.low_range_value and a.{reading_type}<=b.high_range_value) \
    and b.alert_flag = 1"  

    return query

# create dataframe from abnormal bp reading
bp_df = spark.sql(get_df_query('bp'))


# create dataframe from abnormal heart beat reading
heartBeat_df = spark.sql(get_df_query('heartBeat'))

# merging of bp and heartBeat dataframe 
final_df = bp_df.union(heartBeat_df)


# final merged_df with datastream generation time
alertDF = final_df \
    .withColumn("alert_generated_time", current_timestamp() ) \
    .selectExpr("patientname","age","patientaddress","phone_number", "admitted_ward","bp","heartBeat", "input_message_time","alert_generated_time","alert_message")


# conversion of alert stream to json format for streaming   
alertDFJson = alertDF.selectExpr("to_json(struct(*)) as value")


# Console Output of Alert Stream
alertStreamConsole = alertDFJson \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 seconds") \
       .start()


# Producing Alert Stream to to another kafka topic    
alertStreamTopic= alertDFJson \
		.writeStream  \
		.outputMode("append")  \
		.format("kafka")  \
		.option("kafka.bootstrap.servers", kafka_bootstrap_server) \
		.option("topic", kafka_topic)  \
		.option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="1 seconds") \
		.start()
	

# Terminations of topic queue        
alertStreamConsole.awaitTermination()

alertStreamTopic.awaitTermination()


    
    

