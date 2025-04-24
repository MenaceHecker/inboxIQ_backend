import boto3

sns = boto3.client('sns')
topic_arn = "arn:aws:sns:your-region:your-account-id:urgent-emails"

def lambda_handler(event, context):
    for record in event["Records"]:
        if "urgent" in record["subject"].lower():
            sns.publish(
                TopicArn=topic_arn,
                Message=f"Urgent Email: {record['subject']}",
                Subject="Urgent Email Alert"
            )
    return {"status": "Notification Sent"}
