import json
import logging
import boto3
import os
import pandas as pd
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sqs = boto3.client("sqs")
s3 = boto3.client("s3")


# Extraer emails de reservas
df = pd.read_excel("Actividades.xlsx")
email_val = set(df["Emails_Reservas"].dropna().str.strip())


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
                    QueueUrl=queue_url, MessageBody=record["body"]
                )
                logger.info(f"Mensaje enviado a SQS con ID: {response['MessageId']}")
            except Exception:
                logger.exception("Exception sending the message")
        else:
            logger.info("Email not to be processed")
            try:
                # Generar la nueva clave reemplazando "emails/" por "no_relevante/"
                new_key = s3_object.replace("emails/", "no_relevante/")
                # Copiar el objeto a la nueva ubicación
                s3.copy_object(
                    Bucket=s3_bucket,
                    CopySource={"Bucket": s3_bucket, "Key": s3_object},
                    Key=new_key,
                )
                # Eliminar el objeto original en "emails/"
                s3.delete_object(Bucket=s3_bucket, Key=s3_object)
                logger.info(f"Email moved to no_relevante folder: {new_key}")
            except Exception:
                logger.exception("Error moving the email")
            return {"statusCode": 200, "body": "Mensaje procesado"}


def read_email_in_s3(bucket_name, s3_key):
    response = s3.get_object(Bucket=bucket_name, Key=s3_key)
    file_content = response["Body"].read().decode("utf-8")
    return file_content


def should_email_be_processed(email_content):
    """
    Extrayendo el remitente del email.
    """
    match = re.search(r"Return-Path:\s*<?([^>]+)>?", email_content, re.IGNORECASE)
    if match:
        if match.group(1).strip() in email_val:
            logger.info(
                f"El email {match.group(1).strip()} está en la lista de emails de reservas"
            )
            return True
        else:
            logger.info(
                f"El email {match.group(1).strip()} no está en la lista de emails de reservas"
            )
            return False
        # Si no se encuentra un remitente, no se procesa
    logger.info("No se encontró un remitente en el email.")
    return False
