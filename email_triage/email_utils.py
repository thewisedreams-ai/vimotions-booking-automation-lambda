from email import policy
from email.parser import BytesParser
from email.utils import getaddresses
import logging
import os
import re
import time
import uuid
import boto3
from botocore.exceptions import ClientError
import json
from io import BytesIO
import pandas as pd

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicialización de clientes AWS
sqs = boto3.client("sqs")
s3 = boto3.client("s3")
# Cargar lista de emails válidos desde Excel
environment = os.environ.get("Environment", "test")

# Inicializar el recurso de DynamoDB (ajusta la región según corresponda)
dynamodb = boto3.resource("dynamodb", region_name="eu-west-1")

# Obtener el nombre de la tabla desde una variable de entorno o usar el valor por defecto
email_table_name = os.getenv(
    "DYNAMO_EMAIL_TABLE", "agents-test-booking-agent-email-booking"
)
email_table = dynamodb.Table(email_table_name)
# Cargar la lista de emails de reservas desde un archivo Excel en S3
EMAIL_VAL = set()  # Inicializar el conjunto vacío

try:
    # Realiza el scan de la tabla
    response = email_table.scan()
    items = response.get("Items", [])
    # Si se requiere paginación:
    while "LastEvaluatedKey" in response:
        response = email_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    # Extrae la key "email" de cada ítem
    EMAIL_VAL = set(item["email"] for item in items if "email" in item)
    logger.info(f"Lista de emails cargados desde DynamoDB: {EMAIL_VAL}")
except Exception as e:
    logger.error(f"Error al cargar emails desde DynamoDB: {e}")

# def send_queue_message(queue_url, msg_attributes, msg_body, message_group_id="default"):
#     """
#     Sends a message to the specified queue.
#     """
#     try:
#         response = sqs.send_message(
#             QueueUrl=queue_url,
#             MessageAttributes=msg_attributes,
#             MessageBody=msg_body,
#             # MessageGroupId=message_group_id,
#             # MessageDeduplicationId=deduplication_id,
#         )
#     except ClientError:
#         logger.exception(f"Could not send meessage to the - {queue_url}.")
#         raise
#     else:
#         return response


# def acquire_lock(resource_id, owner_id, ttl_seconds=60):
#     """Adquiere un bloqueo para el recurso especificado"""
#     dynamodb = boto3.resource("dynamodb")
#     table = dynamodb.Table("ResourceLocks")

#     try:
#         # Intenta crear un registro de bloqueo con condición de que no exista
#         response = table.put_item(
#             Item={
#                 "resource_id": resource_id,
#                 "owner_id": owner_id,
#                 "expiration_time": int(time.time()) + ttl_seconds,
#             },
#             ConditionExpression="attribute_not_exists(resource_id) OR expiration_time < :now",
#             ExpressionAttributeValues={":now": int(time.time())},
#         )
#         logger.info(f"Bloqueo adquirido para {resource_id} por {owner_id}")
#         return True
#     except ClientError as e:
#         if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
#             logger.warning(
#                 f"No se pudo adquirir el bloqueo para {resource_id}, ya está bloqueado"
#             )
#             return False
#         raise


# def release_lock(resource_id, owner_id):
#     """Libera un bloqueo si es el propietario"""
#     dynamodb = boto3.resource("dynamodb")
#     table = dynamodb.Table("ResourceLocks")

#     try:
#         response = table.delete_item(
#             Key={"resource_id": resource_id},
#             ConditionExpression="owner_id = :owner_id",
#             ExpressionAttributeValues={":owner_id": owner_id},
#         )
#         logger.info(f"Bloqueo liberado para {resource_id} por {owner_id}")
#         return True
#     except ClientError as e:
#         if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
#             logger.warning(
#                 f"No se pudo liberar el bloqueo para {resource_id}, no eres el propietario"
#             )
#             return False
#         raise


# def read_excel_s3(bucket, key):
#     """Lee un archivo Excel desde S3 con parámetros para evitar caché"""
#     s3 = boto3.client("s3")
#     timestamp = int(time.time())  # Para evitar caché

#     # Añadir parámetros para evitar caché
#     obj = s3.get_object(
#         Bucket=bucket, Key=key, ResponseCacheControl="no-cache,no-store,must-revalidate"
#     )
#     return pd.read_excel(BytesIO(obj["Body"].read()))


# def read_excel_s3_with_lock(bucket, key, max_retries=5, retry_delay=2):
#     """Lee un archivo Excel de S3 con bloqueo"""
#     execution_id = str(uuid.uuid4())
#     resource_id = f"{bucket}/{key}"

#     for attempt in range(max_retries):
#         if acquire_lock(resource_id, execution_id, ttl_seconds=30):
#             try:
#                 try:
#                     df = read_excel_s3(bucket, key)
#                     logger.info(f"Archivo leído correctamente: {bucket}/{key}")
#                     return df
#                 except ClientError as e:
#                     if e.response["Error"]["Code"] == "NoSuchKey":
#                         logger.warning(
#                             f"El archivo {key} no existe en el bucket {bucket}"
#                         )
#                         return pd.DataFrame()  # Devuelve DataFrame vacío si no existe
#                     raise
#             finally:
#                 release_lock(resource_id, execution_id)

#         logger.warning(
#             f"Intento {attempt + 1}/{max_retries} fallido para obtener bloqueo de lectura para {key}"
#         )
#         time.sleep(retry_delay)

#     raise Exception(
#         f"No se pudo adquirir el bloqueo para leer {resource_id} después de {max_retries} intentos"
#     )


def extract_email_headers(msg):
    """
    Extrae los headers relevantes: From, To, Cc, Bcc y Subject.
    Se convierten las direcciones a una lista de tuplas (nombre, email).
    """
    headers = {}
    headers["from"] = getaddresses([msg.get("From", "")])
    headers["to"] = getaddresses(msg.get_all("To", []))
    headers["cc"] = getaddresses(msg.get_all("Cc", []))
    headers["bcc"] = getaddresses(msg.get_all("Bcc", []))
    headers["subject"] = msg.get("Subject", "")
    return headers


def extract_email_body(msg):
    """
    Extrae todo el contenido del email:
    - 'plain': texto plano
    - 'html': versión en HTML
    """
    email_body_plain = ""
    email_body_html = ""

    if msg.is_multipart():
        # Recorremos todas las partes del mensaje
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                try:
                    email_body_plain += part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="replace"
                    )
                except Exception as e:
                    logger.error("Error decodificando text/plain: " + str(e))
            elif content_type == "text/html":
                try:
                    email_body_html += part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="replace"
                    )
                except Exception as e:
                    logger.error("Error decodificando text/html: " + str(e))
    else:
        # Caso de email no multipart
        content_type = msg.get_content_type()
        try:
            if content_type == "text/plain":
                email_body_plain = msg.get_payload(decode=True).decode(
                    msg.get_content_charset() or "utf-8", errors="replace"
                )
            elif content_type == "text/html":
                email_body_html = msg.get_payload(decode=True).decode(
                    msg.get_content_charset() or "utf-8", errors="replace"
                )
        except Exception as e:
            logger.error("Error decodificando el contenido: " + str(e))

    return {"plain": email_body_plain, "html": email_body_html}


def read_email_in_s3(bucket_name, s3_key):
    """
    Obtiene el objeto de S3 y retorna el email procesado.
    """
    try:
        s3_object = s3.get_object(Bucket=bucket_name, Key=s3_key)
        raw_email = s3_object["Body"].read()
        msg = BytesParser(policy=policy.default).parsebytes(raw_email)
        headers = extract_email_headers(msg)
        body = extract_email_body(msg)
        logger.info("Remitente(s): %s", headers["from"])
        logger.info("Asunto: %s", headers["subject"])
        logger.info("Cuerpo (texto plano, primeros 500): %s", body["plain"][:500])
        logger.info("Cuerpo (HTML, primeros 500): %s", body["html"][:500])

        return {"headers": headers, "body": body}
    except Exception as e:
        logger.error("Error leyendo email desde S3: " + str(e))
        return None


def should_email_be_processed(email_content):
    """
    Extrae el remitente del email buscando el header "From:" en el contenido y
    verifica si se encuentra en la lista de emails de reservas.
    """
    match = re.search(r"^From:.*<([^>]+)>", email_content, re.IGNORECASE | re.MULTILINE)
    if match:
        sender = match.group(1).strip()
        if sender in EMAIL_VAL:
            logger.info(f"El email {sender} está en la lista de emails de reservas")
            return True
        else:
            logger.info(f"El email {sender} no está en la lista de emails de reservas")
            return False
    else:
        logger.info("No se encontró el header From en el email")
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
