import os

class MailgunClientSingleton:
    _mg_client = None

    def get_client(self):
        if self._mg_client  is None:
            # Initialize the client only once
            mailgunEndpoint = os.getenv('SECRET_MAILGUN_ENDPOINT')
            mailgunApiKey = os.getenv('SECRET_MAILGUN_API_KEY')
            
            if (not mailgunApiKey or not mailgunEndpoint):
                raise ValueError("missing environment variables")

            from mailgun.client import Client
            self._mg_client = Client(auth=("api", mailgunApiKey), api_url=mailgunEndpoint)

            print(f"MailGun Client initiated")
        return self._mg_client 
    

# Create the single instance of MailgunClientSingleton
mailgun_client_instance = MailgunClientSingleton()
