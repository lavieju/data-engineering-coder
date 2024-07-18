from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import os
from dotenv import load_dotenv

load_dotenv() 

password = os.environ.get("secret_pass_gmail")
smtp_server = 'smtp.gmail.com'
smtp_port = 587
sender_email = os.environ.get("sender_email")
send_to = os.environ.get("send_to")


class InsertionResult:
    def __init__(self, status: str, message: str, rows_loaded: int = 0, error=None):
        self.status = status
        self.message = message
        self.rows_loaded = rows_loaded
        self.error = error

    def to_dict(self):
        return {
            'status': self.status,
            'message': self.message,
            'rows_loaded': self.rows_loaded,
            'error': self.error
        }


def send_email(insert_result: InsertionResult):
    subject = 'Spotify Data Insertion'
    if insert_result.get("status") == "success":
        try:
            body_text = 'Data has been successfully inserted!'

            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = send_to
            msg['Subject'] = subject
            msg.attach(MIMEText(body_text, 'plain'))
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, password)
                server.send_message(msg)
            print('E-mail has been sent correctly.')

        except Exception as exception:
            print(exception)
            print('There has been a problem sending the email.')

    else: 
        try:
            body_text = f"There has been a problem inserting data. Error message: ${insert_result.get("error")}"

            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = send_to
            msg['Subject'] = subject
            msg.attach(MIMEText(body_text, 'plain'))
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, password)
                server.send_message(msg)
            print('E-mail has been sent correctly.')

        except Exception as exception:
            print(exception)
            print('There has been a problem sending the email.')