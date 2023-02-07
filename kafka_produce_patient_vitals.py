
# importing required modules
import mysql.connector
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# RDS configuration details
Hostname = "upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com"
username = "student"
password = "STUDENT123"
dbname =  "testdatabase"

# kafka environment details for message queueing and streaming 
kafka_bootstrap_servers = ["localhost:9092"]
kafka_topic = "Patients-Vital-Info"

# create kafka producer object
kafka_producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda x: json.dumps(x).encode('utf-8'))

wait = 1   # measure in seconds, wait time between two message pushed to kafka queue

# function to initiate database connection to RDS
def db_connection(host, user, passwd, db):
    conn =  mysql.connector.connect(
        host = host,
        user = user, 
        password = passwd,
        database = db
        ) 
    return conn

# function to fetch data from RDS
def table_fetch(tablename):
    query = "select * from " + tablename + " ;"
    dbcon = None
    cursor = None
    try:
        dbcon = db_connection(Hostname, username, password, dbname)
        # print("connection established")
        cursor = dbcon.cursor()
        cursor.execute(query)
        result = cursor.fetchall() 
        return result 
    except Exception as e:
        return []
    finally:
        if cursor:
            cursor.close()
        if dbcon:
            dbcon.close()
        
# fetch data from RDS and push each row as message to kafka queue        
if __name__ == '__main__':
    dataset = table_fetch("patients_vital_info")
    
    if len(dataset) == 0:
        message = {'customerId': None, 'heartBeat': None, 'bp': None, }
        print(message)
        kafka_producer.send(kafka_topic, message)
    else:
        for row in dataset:
            #print(row) 
            message = { 'customerId': row[0], 'heartBeat': row[1], 'bp': row[2]}
            print(message)
            kafka_producer.send(kafka_topic, message)
            time.sleep(wait)
 
 


