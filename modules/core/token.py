import hmac
import hashlib
import base64
from datetime import datetime, timezone
import os

def create_token(identifier: str, username: str, time_bar: int) -> str:
    """
    Generates a URL-safe Base64 token based on an identifier, username, and time.
    """
    # 1. Get the current UTC timestamp in milliseconds.
    timestamp_millis = int(datetime.now(timezone.utc).timestamp() * 1000)

    # 2. Create the message for the HMAC.
    message = f"{username};{timestamp_millis}"
    
    SECRET_TOKEN_AUTH_SIGN = os.getenv('SECRET_TOKEN_AUTH_SIGN')
    if SECRET_TOKEN_AUTH_SIGN is None:
        raise Exception('Secret for token not found.')
    
    SECRET_TOKEN_AUTH_SIGN = SECRET_TOKEN_AUTH_SIGN.encode('utf-8')

    # 3. Create the HMAC-SHA1 hash.
    # The key and message must be bytes.
    hmac_digest = hmac.new(
        SECRET_TOKEN_AUTH_SIGN,
        message.encode('utf-8'),
        hashlib.sha1
    ).hexdigest()

    # 4. Concatenate all token parts into a single string.
    token_parts = f"{identifier};{username};{timestamp_millis};{time_bar};{hmac_digest}"

    # 5. Encode the token parts to a URL-safe Base64 string.
    # The base64.urlsafe_b64encode() function requires bytes.
    token_base64_bytes = base64.urlsafe_b64encode(token_parts.encode('utf-8'))
    
    # The result needs to be decoded to a string to be returned.
    return token_base64_bytes.decode('utf-8')