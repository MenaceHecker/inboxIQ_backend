import boto3
import json

comprehend = boto3.client('comprehend')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("EmailRecords")

def lambda_handler(event, context):
    for record in event["Records"]:
        email_body = record["email_body"]
        response = comprehend.classify_document(Text=email_body, EndpointArn="your-comprehend-endpoint")

        category = response["Labels"][0]["Name"]
        email_id = record["email_id"]
        
        table.update_item(
            Key={"email_id": email_id},
            UpdateExpression="SET category = :c",
            ExpressionAttributeValues={":c": category}
        )

    return {"status": "Emails categorized"}
