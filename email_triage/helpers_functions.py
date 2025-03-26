import logging
import time
import uuid
import boto3
from botocore.exceptions import ClientError
import json
from io import BytesIO
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sqs = boto3.client("sqs")


def send_queue_message(queue_url, msg_attributes, msg_body, message_group_id="default"):
    """
    Sends a message to the specified queue.
    """
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageAttributes=msg_attributes,
            MessageBody=msg_body,
            # MessageGroupId=message_group_id,
            # MessageDeduplicationId=deduplication_id,
        )
    except ClientError:
        logger.exception(f"Could not send meessage to the - {queue_url}.")
        raise
    else:
        return response


def acquire_lock(resource_id, owner_id, ttl_seconds=60):
    """Adquiere un bloqueo para el recurso especificado"""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("ResourceLocks")

    try:
        # Intenta crear un registro de bloqueo con condición de que no exista
        response = table.put_item(
            Item={
                "resource_id": resource_id,
                "owner_id": owner_id,
                "expiration_time": int(time.time()) + ttl_seconds,
            },
            ConditionExpression="attribute_not_exists(resource_id) OR expiration_time < :now",
            ExpressionAttributeValues={":now": int(time.time())},
        )
        logger.info(f"Bloqueo adquirido para {resource_id} por {owner_id}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning(
                f"No se pudo adquirir el bloqueo para {resource_id}, ya está bloqueado"
            )
            return False
        raise


def release_lock(resource_id, owner_id):
    """Libera un bloqueo si es el propietario"""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("ResourceLocks")

    try:
        response = table.delete_item(
            Key={"resource_id": resource_id},
            ConditionExpression="owner_id = :owner_id",
            ExpressionAttributeValues={":owner_id": owner_id},
        )
        logger.info(f"Bloqueo liberado para {resource_id} por {owner_id}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning(
                f"No se pudo liberar el bloqueo para {resource_id}, no eres el propietario"
            )
            return False
        raise


def read_excel_s3(bucket, key):
    """Lee un archivo Excel desde S3 con parámetros para evitar caché"""
    s3 = boto3.client("s3")
    timestamp = int(time.time())  # Para evitar caché

    # Añadir parámetros para evitar caché
    obj = s3.get_object(
        Bucket=bucket, Key=key, ResponseCacheControl="no-cache,no-store,must-revalidate"
    )
    return pd.read_excel(BytesIO(obj["Body"].read()))


def read_excel_s3_with_lock(bucket, key, max_retries=5, retry_delay=2):
    """Lee un archivo Excel de S3 con bloqueo"""
    execution_id = str(uuid.uuid4())
    resource_id = f"{bucket}/{key}"

    for attempt in range(max_retries):
        if acquire_lock(resource_id, execution_id, ttl_seconds=30):
            try:
                try:
                    df = read_excel_s3(bucket, key)
                    logger.info(f"Archivo leído correctamente: {bucket}/{key}")
                    return df
                except ClientError as e:
                    if e.response["Error"]["Code"] == "NoSuchKey":
                        logger.warning(
                            f"El archivo {key} no existe en el bucket {bucket}"
                        )
                        return pd.DataFrame()  # Devuelve DataFrame vacío si no existe
                    raise
            finally:
                release_lock(resource_id, execution_id)

        logger.warning(
            f"Intento {attempt + 1}/{max_retries} fallido para obtener bloqueo de lectura para {key}"
        )
        time.sleep(retry_delay)

    raise Exception(
        f"No se pudo adquirir el bloqueo para leer {resource_id} después de {max_retries} intentos"
    )
