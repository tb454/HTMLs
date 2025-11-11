import os
import asyncio
import smtplib
from email.message import EmailMessage

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")

async def send_email(to: str, subject: str, body: str):
    msg = EmailMessage()
    msg["From"] = ADMIN_EMAIL
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)

    def _send_email():
        if not SMTP_HOST:
            raise RuntimeError("SMTP_HOST environment variable is not set")

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            if SMTP_USER and SMTP_PASS:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)

    await asyncio.to_thread(_send_email)
