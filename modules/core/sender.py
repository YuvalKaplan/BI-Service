import log
import os
from modules import config
from modules.core.mailgun import mailgun_client_instance

ENV_TYPE = os.environ.get("ENV_TYPE")

development = ENV_TYPE != 'production'


def send_admin(subject: str, message: str, html: str | None = None) -> None:
    """message is the plain-text body; with html, mail clients show that instead."""
    try:
        mg_client = mailgun_client_instance.get_client();
        mgData = {
            "from": config.Sender.FROM,
            "to": config.Sender.TO,
            "cc": config.Sender.CC,
            "subject": subject,
            "text": message,
        }
        if html:
            mgData["html"] = html
        mg_client.messages.create(data=mgData, domain=config.Sender.DOMAIN)

    except Exception as e:
        log.record_error(f"Error sending email to admin: {e}")
