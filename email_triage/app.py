import json
import logging
import os
import re

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from email import policy
from email.parser import BytesParser

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicialización de clientes AWS
sqs = boto3.client("sqs")
s3 = boto3.client("s3")

# Cargar lista de emails válidos desde Excel
EXCEL_FILE = "Actividades.xlsx"
df = pd.read_excel(EXCEL_FILE)
EMAIL_VAL = set(df["Emails_Reservas"].dropna().str.strip())
logger.info(f"Lista de emails cargados desde Excel: {EMAIL_VAL}")


def extract_email_body(raw_email_bytes):
    """
    Parsea los bytes del email y retorna un string formateado con el remitente, asunto y cuerpo.
    """
    email_message = BytesParser(policy=policy.default).parsebytes(raw_email_bytes)
    sender = email_message.get("From", "")
    subject = email_message.get("Subject", "")
    # Se intenta obtener la parte de texto plano, y en su defecto la HTML
    body_part = email_message.get_body(preferencelist=("plain", "html"))
    body = body_part.get_content() if body_part else email_message.get_content()
    return f"From: {sender}\nSubject: {subject}\n\n{body}"


def read_email_in_s3(bucket_name, s3_key):
    """
    Obtiene el objeto de S3 y retorna el email procesado.
    """
    s3_object = s3.get_object(Bucket=bucket_name, Key=s3_key)
    raw_email = s3_object["Body"].read()
    return extract_email_body(raw_email)


def should_email_be_processed(email_content):
    """
    Extrae el remitente del email y lo compara con la lista de emails válidos.
    Primero intenta extraerlo usando el header "From:" y, si no se encuentra, usa "Return-Path:".
    """
    sender = None

    # Buscar remitente en el header "From:"
    pattern_from = r"From:\s*(?:\"[^\"]+\"\s*)?<([^>\s]+)>"
    match = re.search(pattern_from, email_content, re.IGNORECASE)
    if match:
        sender = match.group(1).strip()
    else:
        # Fallback: buscar en "Return-Path:"
        pattern_return = r"Return-Path:\s*<?([^>]+)>?"
        match = re.search(pattern_return, email_content, re.IGNORECASE)
        if match:
            sender = match.group(1).strip()

    if sender:
        if sender in EMAIL_VAL:
            logger.info(f"El email {sender} está en la lista de emails de reservas")
            return True
        else:
            logger.info(f"El email {sender} no está en la lista de emails de reservas")
            return False

    logger.info("No se encontró un remitente en el email.")
    return False


def send_queue_message(queue_url, msg_attributes, msg_body):
    """
    Envía un mensaje a la cola SQS especificada.
    """
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageAttributes=msg_attributes,
            MessageBody=msg_body,
        )
        return response
    except ClientError:
        logger.exception(f"Could not send message to the queue: {queue_url}.")
        raise


def move_email_to_no_relevante(s3_bucket, s3_object):
    """
    Mueve el objeto del email de la carpeta "emails/" a "no_relevante/" en S3.
    """
    try:
        new_key = s3_object.replace("emails/", "no_relevante/")
        s3.copy_object(
            Bucket=s3_bucket,
            CopySource={"Bucket": s3_bucket, "Key": s3_object},
            Key=new_key,
        )
        s3.delete_object(Bucket=s3_bucket, Key=s3_object)
        logger.info(f"Email moved to no_relevante folder: {new_key}")
    except Exception:
        logger.exception("Error moving the email")


def lambda_handler(event, context):
    """
    Función principal Lambda que procesa los emails recibidos a través de SQS.
    """
    for record in event.get("Records", []):
        message = json.loads(record.get("body", "{}"))
        action_info = message.get("receipt", {}).get("action", {})
        s3_bucket = action_info.get("bucketName")
        s3_object = action_info.get("objectKey")

        if not s3_bucket or not s3_object:
            logger.error("Falta el bucket o la clave del objeto en el mensaje SQS")
            continue

        email_content = read_email_in_s3(s3_bucket, s3_object)
        logger.info(f"Email content: {email_content}")

        if should_email_be_processed(email_content):
            queue_url = os.environ.get("SQS_URL")
            logger.info("Preparando mensaje para enviar a la cola SQS")

            try:
                response = send_queue_message(
                    queue_url,
                    {"email": {"DataType": "String", "StringValue": "email"}},
                    email_content,
                )
                logger.info(
                    f"Mensaje enviado a Queue con ID: {response.get('MessageId')}"
                )
            except Exception:
                logger.exception("Exception sending the message")
        else:
            logger.info("El email no será procesado; moviendo a carpeta no_relevante")
            move_email_to_no_relevante(s3_bucket, s3_object)

    return {"statusCode": 200, "body": "Mensaje procesado"}
