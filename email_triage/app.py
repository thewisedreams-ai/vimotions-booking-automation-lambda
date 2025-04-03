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

# # Cargar lista de emails válidos desde Excel
# environment = os.environ.get("Environment", "test")

# # Inicializar el recurso de DynamoDB (ajusta la región según corresponda)
# dynamodb = boto3.resource("dynamodb", region_name="eu-west-1")

# # Obtener el nombre de la tabla desde una variable de entorno o usar el valor por defecto
# email_table_name = os.getenv(
#     "DYNAMO_EMAIL_TABLE", "agents-test-booking-agent-email-booking"
# )
# email_table = dynamodb.Table(email_table_name)
# # Cargar la lista de emails de reservas desde un archivo Excel en S3
# EMAIL_VAL = set()  # Inicializar el conjunto vacío

# try:
#     # Realiza el scan de la tabla
#     response = email_table.scan()
#     items = response.get("Items", [])
#     # Si se requiere paginación:
#     while "LastEvaluatedKey" in response:
#         response = email_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
#         items.extend(response.get("Items", []))

#     # Extrae la key "email" de cada ítem
#     EMAIL_VAL = set(item["email"] for item in items if "email" in item)
#     logger.info(f"Lista de emails cargados desde DynamoDB: {EMAIL_VAL}")
# except Exception as e:
#     logger.error(f"Error al cargar emails desde DynamoDB: {e}")


# def extract_email_headers(msg):
#     """
#     Extrae los headers relevantes: From, To, Cc, Bcc y Subject.
#     Se convierten las direcciones a una lista de tuplas (nombre, email).
#     """
#     headers = {}
#     headers["from"] = getaddresses([msg.get("From", "")])
#     headers["to"] = getaddresses(msg.get_all("To", []))
#     headers["cc"] = getaddresses(msg.get_all("Cc", []))
#     headers["bcc"] = getaddresses(msg.get_all("Bcc", []))
#     headers["subject"] = msg.get("Subject", "")
#     return headers


# def extract_email_body(msg):
#     """
#     Extrae todo el contenido del email:
#     - 'plain': texto plano
#     - 'html': versión en HTML
#     """
#     email_body_plain = ""
#     email_body_html = ""

#     if msg.is_multipart():
#         # Recorremos todas las partes del mensaje
#         for part in msg.walk():
#             content_type = part.get_content_type()
#             if content_type == "text/plain":
#                 try:
#                     email_body_plain += part.get_payload(decode=True).decode(
#                         part.get_content_charset() or "utf-8", errors="replace"
#                     )
#                 except Exception as e:
#                     logger.error("Error decodificando text/plain: " + str(e))
#             elif content_type == "text/html":
#                 try:
#                     email_body_html += part.get_payload(decode=True).decode(
#                         part.get_content_charset() or "utf-8", errors="replace"
#                     )
#                 except Exception as e:
#                     logger.error("Error decodificando text/html: " + str(e))
#     else:
#         # Caso de email no multipart
#         content_type = msg.get_content_type()
#         try:
#             if content_type == "text/plain":
#                 email_body_plain = msg.get_payload(decode=True).decode(
#                     msg.get_content_charset() or "utf-8", errors="replace"
#                 )
#             elif content_type == "text/html":
#                 email_body_html = msg.get_payload(decode=True).decode(
#                     msg.get_content_charset() or "utf-8", errors="replace"
#                 )
#         except Exception as e:
#             logger.error("Error decodificando el contenido: " + str(e))

#     return {"plain": email_body_plain, "html": email_body_html}


# def read_email_in_s3(bucket_name, s3_key):
#     """
#     Obtiene el objeto de S3 y retorna el email procesado.
#     """
#     try:
#         s3_object = s3.get_object(Bucket=bucket_name, Key=s3_key)
#         raw_email = s3_object["Body"].read()
#         msg = BytesParser(policy=policy.default).parsebytes(raw_email)
#         headers = extract_email_headers(msg)
#         body = extract_email_body(msg)
#         logger.info("Remitente(s): %s", headers["from"])
#         logger.info("Asunto: %s", headers["subject"])
#         logger.info("Cuerpo (texto plano, primeros 500): %s", body["plain"][:500])
#         logger.info("Cuerpo (HTML, primeros 500): %s", body["html"][:500])

#         return {"headers": headers, "body": body}
#     except Exception as e:
#         logger.error("Error leyendo email desde S3: " + str(e))
#         return None


# def should_email_be_processed(email_content):
#     """
#     Extrae el remitente del email buscando el header "From:" en el contenido y
#     verifica si se encuentra en la lista de emails de reservas.
#     """
#     match = re.search(r"^From:.*<([^>]+)>", email_content, re.IGNORECASE | re.MULTILINE)
#     if match:
#         sender = match.group(1).strip()
#         if sender in EMAIL_VAL:
#             logger.info(f"El email {sender} está en la lista de emails de reservas")
#             return True
#         else:
#             logger.info(f"El email {sender} no está en la lista de emails de reservas")
#             return False
#     else:
#         logger.info("No se encontró el header From en el email")
#         return False


# def send_queue_message(queue_url, msg_attributes, msg_body):
#     """
#     Envía un mensaje a la cola SQS especificada.
#     """
#     try:
#         response = sqs.send_message(
#             QueueUrl=queue_url,
#             MessageAttributes=msg_attributes,
#             MessageBody=msg_body,
#         )
#         return response
#     except ClientError:
#         logger.exception(f"Could not send message to the queue: {queue_url}.")
#         raise


# def move_email_to_no_relevante(s3_bucket, s3_object):
#     """
#     Mueve el objeto del email de la carpeta "emails/" a "no_relevante/" en S3.
#     """
#     try:
#         new_key = s3_object.replace("emails/", "no_relevante/")
#         s3.copy_object(
#             Bucket=s3_bucket,
#             CopySource={"Bucket": s3_bucket, "Key": s3_object},
#             Key=new_key,
#         )
#         s3.delete_object(Bucket=s3_bucket, Key=s3_object)
#         logger.info(f"Email moved to no_relevante folder: {new_key}")
#     except Exception:
#         logger.exception("Error moving the email")


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

        if should_email_be_processed(combined_email):
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
