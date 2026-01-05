import log
import os
from modules.core.mailgun import mailgun_client_instance

ENV_TYPE = os.environ.get("ENV_TYPE")

development = ENV_TYPE != 'production'

TTL_TOKEN = 60 * 24 * 7
DOMAIN = 'insightful.investments'

def send_admin(subject: str, message: str):
    try:
        mg_client = mailgun_client_instance.get_client();
        mgData = {
            "from": f"Best Ideas Admin <admin@{DOMAIN}>",
            "to": f"yuval.kaplan@{DOMAIN}",
            # "cc": f"oren.kaplan@{DOMAIN}",
            "subject": subject,
            "text": message,
        }
        mg_client.messages.create(data=mgData, domain=DOMAIN)

    except Exception as e:
        log.record_error(f"Error sending email to admin: {e}")
