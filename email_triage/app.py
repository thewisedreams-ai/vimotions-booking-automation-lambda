import json
import logging
import boto3
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sqs = boto3.client('sqs')
s3 = boto3.client('s3')


def lambda_handler(event, context):
    for record in event["Records"]:
        message = json.loads(record["body"])
        action_info = message.get("receipt").get("action")
        s3_bucket = action_info.get("bucketName")
        s3_object = action_info.get("objectKey")
        email_content = read_email_in_s3(s3_bucket, s3_object)
        if should_email_be_processed(email_content):
            queue_url = os.environ.get("SQS_URL")
            logger.info("Message to be sent")
            try:
                response = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=record["body"]
                )
                logger.info(f"Mensaje enviado a SQS con ID: {response['MessageId']}")
            except Exception:
                logger.exception("Exception sending the message")
        else:
            logger.info("Email not to be processed")
            #borrar el email o moverlo a otra carpeta
    return {"statusCode": 200, "body": "Mensaje procesado"}


def read_email_in_s3(bucket_name, s3_key):
    response = s3.get_object(Bucket=bucket_name, Key=s3_key)
    file_content = response['Body'].read().decode('utf-8')
    return file_content

def should_email_be_processed(email_content):
    return True
