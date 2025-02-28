# Booking automation

This project is designed to process incoming emails using Amazon Web Services (AWS). 
The architecture utilizes AWS **Simple Email Service (SES)** for receiving emails, **S3** for storing, **SNS** to notify other services, **SQS** as the message queue, and **Lambda** to check if the email needs to be processed by our ai agent. 
The Lambda function is triggered by messages from SQS.

### Resources

- **SES**: Configured to receive emails sent to a specific email address.
- **S3 Bucket**: Stores incoming email content and attachments.
- **SNS Topic**: Sends notifications when a new email arrives.
- **SQS Queue**: Receives notifications from SNS and triggers Lambda.
- **Lambda Function**: Processes the messages from SQS and check if the message should be processed. In this case, a message is sent to the follow SQS queue.
- **SQS Queue**: Receives message from the previous lambdas with the emails should be processed. This messages will be read by our ai agent.
---

## Setup Instructions

### Prerequisites

- AWS CLI installed and configured on your machine.
- AWS SAM (Serverless Application Model) installed.

### Deploy the SAM Application

   To deploy the application using AWS SAM, follow these steps:

   - **Build the SAM Application**:

     ```bash
     sam build
     ```

   - **Deploy the SAM Application**:

     ```bash
     sam deploy --config-env test
     ```

5. **Verify Email Reception**

   After deployment, verify that SES is receiving emails sent to the configured address. Ensure that the emails are being stored in the S3 bucket and that SNS notifications are being sent.

6. **Check SQS and Lambda**

   - Check the **SQS Queue** to see if it receives the SNS notifications.
   - Check the **Lambda function logs** in **CloudWatch** to verify the processing of the incoming email.

---

## Testing

1. **Test with SES**: Send an email to the configured email address. Check that it is stored in the S3 bucket and triggers the SNS notification.
2. **Check SQS**: Verify that the SQS queue receives the message from SNS.
3. **Check Lambda Logs**: Review the Lambda logs in CloudWatch to ensure the function is processing the email data correctly.

---
