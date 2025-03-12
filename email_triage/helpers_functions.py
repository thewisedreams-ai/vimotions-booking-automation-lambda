import logging
import boto3
from botocore.exceptions import ClientError
import json

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
