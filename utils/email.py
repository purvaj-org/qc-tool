import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from .database import get_db_connection

# Email configuration
SMTP_SERVER = "smtp.sendgrid.net"
SMTP_PORT = 587
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

def get_admin_email():
    """Get admin email from database."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT email FROM user_table WHERE user_role = 'admin' LIMIT 1")
            admin = cursor.fetchone()
            return admin['email'] if admin else None
    finally:
        conn.close()

def send_email(subject, body, to_email):
    """Send email notification."""
    if not to_email:
        print("Email error: No admin email found")
        return False
    try:
        msg = MIMEMultipart()
        msg['From'] = "noreply@purvaj.com"
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        print(f"Email sent to {to_email}: {subject}")
        return True
    except Exception as e:
        print(f"Email error: Failed to send email - {str(e)}")
        return False 