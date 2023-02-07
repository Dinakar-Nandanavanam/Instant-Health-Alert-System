
# importing required modules
import sys
import os
import time
import re
import json
import boto3
from kafka import KafkaConsumer
from datetime import datetime


# kafka environment details for consuming input topic stram
kafka_bootstrap_server = "localhost:9092"
kafka_topic = "Health-Alert-Messages"

# create kafka consumer object
kafka_consumer = KafkaConsumer(kafka_topic, bootstrap_servers = kafka_bootstrap_server, auto_offset_reset = 'earliest')


# sns configuration details
sns_topic = 'Health-Alert-Notification'
region = "us-east-1"

# aws_session = boto3.Session()
# aws_credentials = aws_session.get_credentials()
# aws_access_key = aws_credentials.access_key
# aws_secret_key = aws_credentials.secret_key

aws_access_key = r"AKIA3ZOBXQLKCKEFYS5P"
aws_secret_key = r"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"


# check provided subscriber email id validity
def check_invalid_email(email_id):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    status = re.match(pattern, email_id) is None
    return status


# create sns client
def init_sns_client(access_key, secret_key):
    sns_client = boto3.client('sns',aws_access_key_id = access_key, aws_secret_access_key = secret_key, region_name = region) 
    return sns_client

# create sns topic and subscriber
def init_topic_subscription(snsClient, endpoint_mail_id):
    sns_topic_obj = snsClient.create_topic(Name=sns_topic)
    sns_topic_arn = sns_topic_obj['TopicArn']

    sns_subscription_obj = snsClient.subscribe(
    TopicArn=sns_topic_arn,
    Protocol='email',
    Endpoint=endpoint_mail_id,
    ReturnSubscriptionArn=True
    )
        
    sns_subscription_arn = sns_subscription_obj['SubscriptionArn']
    
    # on successfull subscriber creation, waiting for user to verify through mail.
        
    poll_count = 0
    while True:
        pendig_confirm_status = snsClient.get_subscription_attributes(SubscriptionArn=sns_subscription_arn)['Attributes']['PendingConfirmation']
        if pendig_confirm_status == 'true':
            if poll_count == 0:
                print(f'\nSNS topic subscription verification mail sent to {endpoint_mail_id} \nPlease check mailbox. Verification mandatory before proceeding.')
                print('\nSubscription verification pending...')   
        else:
            print('\nSNS topic subscriber mail verification done. Alert publishing will be commenced.\nAfter completion please press ctrl-c to delete subscriber,topic automatically. \n')
            break
        poll_count+=1
        time.sleep(5)
        
    return sns_topic_arn, sns_subscription_arn
        

# deletion of created subscriber and topic
def delete_topic_subscription(snsClient, topic, subscription):    
    try:
        del_subscriber = snsClient.unsubscribe(SubscriptionArn=subscription)
        del_topic = snsClient.delete_topic(TopicArn=topic)
        print('\ntopic and subscriber deleted assuming it is no longer needed. \n')
    except:
        print('failed to delete topic and subscriber. Manually delete if exists and no longer in use.')
     

# format message in json format to multiline message body text
def format_message_body(msg_obj):
    body_lines = ['Alert Details:- \n']
    for key, value in msg_obj.items():
        line = f'{key}: {value}'
        body_lines.append(line)        
    body_text = '\n'.join(body_lines)
    return body_text
        

# function to consume message from kafka topic and publish to sns topic subscriber         
def pulish_alert(snsClient, sns_topic_arn, stream):
    message_obj = json.loads(stream.value)
    subject_header = 'Health Alert: Patient- ' + message_obj['patientname']
    message_body = format_message_body(message_obj)
    print(message_obj)

    response = snsClient.publish(TopicArn = sns_topic_arn, Message=  message_body,  Subject = subject_header )
    


# main thread for publishing operation   
if __name__ == '__main__':
    email_id = input('Enter a valid Email Id for SNS topic subscription: ').strip()
    
    if check_invalid_email(email_id):
        print(f'{email_id} is not a valid email id. run script with valid email id')
        sys.exit()
        
    try:
        sns_client = init_sns_client(aws_access_key, aws_secret_key)
    except:
        print('Unable to initiate sns client. Reason could be invalid access_key, secrete_key. Provide correnct credentials.')
        sys.exit()
            
    try:
        sns_topic_arn, sns_subscription_arn = init_topic_subscription(sns_client, email_id)
    except:
        print('Failed to create sns topic-subscriber')     
    
    try:
        for message in kafka_consumer:
            pulish_alert(sns_client, sns_topic_arn, message)                 
    except KeyboardInterrupt:
            print('\nHealth Alert Notification sending to mail done.')
            delete_topic_subscription(sns_client, sns_topic_arn, sns_subscription_arn)
            sys.exit()

        