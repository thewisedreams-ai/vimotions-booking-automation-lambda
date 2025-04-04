import json
import logging
import os
from email_utils import (
    read_email_in_s3,
    should_email_be_processed,
    send_queue_message,
    move_email_to_no_relevante,
)

import boto3

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicialización de clientes AWS
sqs = boto3.client("sqs")
s3 = boto3.client("s3")

dynamodb = boto3.resource("dynamodb", region_name="eu-west-1")

# Obtener el nombre de la tabla desde una variable de entorno o usar el valor por defecto
email_table_name = os.getenv(
    "DYNAMO_EMAIL_TABLE", "agents-test-booking-agent-email-booking"
)
email_table = dynamodb.Table(email_table_name)
EMAIL_VAL = set()


def load_valid_emails():
    EMAIL_VAL = set()
    try:
        response = email_table.scan()
        items = response.get("Items", [])
        while "LastEvaluatedKey" in response:
            response = email_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))
        EMAIL_VAL = set(item["email"] for item in items if "email" in item)
        logger.info(f"Lista de emails cargados desde DynamoDB: {EMAIL_VAL}")
    except Exception as e:
        logger.error(f"Error al cargar emails desde DynamoDB: {e}")
    return EMAIL_VAL


def lambda_handler(event, context):
    """
    Función principal Lambda que procesa los emails recibidos a través de SQS.
    """
    EMAIL_VAL = load_valid_emails()

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

        if not email_content:
            logger.warning("No se pudo cargar el email desde S3.")
            continue

        # Extraer headers y cuerpo
        headers = email_content["headers"]
        body = email_content["body"]

        # Convertir las listas de direcciones en cadenas legibles.
        from_emails = ", ".join(
            [f"{name} <{email}>" if name else email for name, email in headers["from"]]
        )
        to_emails = ", ".join(
            [f"{name} <{email}>" if name else email for name, email in headers["to"]]
        )
        subject = headers["subject"]

        # Seleccionar el cuerpo: se prefiere el texto plano; si no existe, se usa el HTML.
        email_body = body["plain"] if body["plain"] else body["html"]

        # Crear el email combinado con el formato solicitado.
        combined_email = (
            f"From: {from_emails}\n"
            f"To: {to_emails}\n"
            f"Subject: {subject}\n"
            f"Body: {email_body}"
        )

        logger.info("----- Email Combinado -----")
        logger.info(combined_email)

        if should_email_be_processed(combined_email, EMAIL_VAL):
            queue_url = os.environ.get("SQS_URL")
            logger.info("Preparando mensaje para enviar a la cola SQS")

            try:
                response = send_queue_message(
                    queue_url,
                    {"email": {"DataType": "String", "StringValue": "email"}},
                    combined_email,
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
