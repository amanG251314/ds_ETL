#Importing Libraries
from google.cloud import secretmanager
from google.oauth2.credentials import Credentials
import os
from ast import literal_eval
from googleapiclient.discovery import build
from email.mime.text import MIMEText
import base64
from config import *
from dotenv import load_dotenv
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/aakash/mp360/credentials/dev.json"

#Gettting Environment Variables
project_id = os.getenv('project_id')

def mail(subject,msg,receivers=mailingList):
        # fetching from secret manager
        ds_gmail_secret_id_SM = "DS_GMAIL_TOKEN"
        #Create the Secret Manager client.
        secret_manager_client = secretmanager.SecretManagerServiceClient()
        # gmail token - fetching secrets from secret manager
        response_gmail = secret_manager_client.access_secret_version(
            name = f'projects/{project_id}/secrets/{ds_gmail_secret_id_SM}/versions/latest'
        )
        payload_gmail = response_gmail.payload.data.decode("UTF-8")

        creds_ = Credentials.from_authorized_user_info(literal_eval(payload_gmail))
        service = build('gmail', 'v1', credentials=creds_)

        sender = 'data.science@merapashu360.com'

        creds_ = Credentials.from_authorized_user_info(literal_eval(payload_gmail))

        service = build('gmail', 'v1', credentials=creds_)

        for receiver in receivers:
            message = MIMEText(msg)
            message['to'] = receiver
            message['from'] = sender
            message['subject'] = subject
            raw = base64.urlsafe_b64encode(message.as_bytes())
            raw = raw.decode()
            body = {'raw' : raw}
            message = (service.users().messages().send(userId='me', body=body).execute())