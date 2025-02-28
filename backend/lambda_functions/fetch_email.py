import os
import pickle
import boto3
import json
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
S3_BUCKET = "email-metadata-bucket"
DYNAMODB_TABLE = "EmailRecords"

def get_credentials():
    """Authenticate user with Gmail OAuth."""
    creds = None
    if os.path.exists('/tmp/token.pickle'):
        with open('/tmp/token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file('/tmp/credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
        with open('/tmp/token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds

def fetch_emails():
    """Fetch emails from Gmail API."""
    creds = get_credentials()
    service = build('gmail', 'v1', credentials=creds)
    results = service.users().messages().list(userId='me', maxResults=5).execute()
    messages = results.get('messages', [])

    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE)

    for msg in messages:
        email_id = msg['id']
        msg_details = service.users().messages().get(userId='me', id=email_id).execute()

        email_data = {
            "email_id": email_id,
            "subject": msg_details["snippet"],
            "raw": json.dumps(msg_details)
        }

        s3.put_object(Bucket=S3_BUCKET, Key=f"{email_id}.json", Body=json.dumps(email_data))
        table.put_item(Item=email_data)

    return {"status": "Emails fetched and stored"}

def lambda_handler(event, context):
    return fetch_emails()
