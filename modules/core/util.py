import hashlib
from datetime import datetime, timezone
from io import BytesIO
import httpx
import random
import string
import bcrypt
import re
from tld import get_fld

def get_consistent_hash(input: str | tuple | list, algorithm='sha256') -> str:
    if algorithm not in hashlib.algorithms_available:
        raise ValueError(f"Algorithm '{algorithm}' not available in hashlib.")

    if isinstance(input, str):
        hash_object = hashlib.new(algorithm)
        hash_object.update(input.encode('utf-8'))
        return hash_object.hexdigest()

    if isinstance(input, tuple):
        hash_object = hashlib.new(algorithm)
        hash_object.update(repr(input).encode('utf-8'))
        return hash_object.hexdigest()
    
    if isinstance(input, list):
        hash_object = hashlib.new(algorithm)
        hash_object.update(repr(tuple(input)).encode('utf-8'))
        return hash_object.hexdigest()
    
def get_file_hash(input: bytes):
    """ Note: This is also used as the Checksum for the CDN upload """
    hash_object = hashlib.new('sha256')
    hash_object.update(input)
    return hash_object.hexdigest()

def get_base_path(client_id):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H-%M-%S")
    return f'./downloads/extracted/{client_id}/{timestamp}/' 

def download_file_from_url(url):
    """Downloads a file from a URL and returns it as a BytesIO object."""
    try:
        return BytesIO(httpx.get(url).content)
    except httpx.HTTPError as e:
        print(f"Error downloading file: {e}")
        return None
    except httpx.InvalidURL as e:
        print(f"Invalid URL: {e}")
        return None

def generate_random_string(length: int) -> str:
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for i in range(length))

def get_password_hashed(password: str) -> str:
    # The website uses the argon2 hashing algorithm to hash asswords.
    # The argon2 requires an install with pip that fails so we are using the bcrypt instead.
    # # This is exceptable as any users that are added to the system will need to use the forgot password the first time they login.
    # This will reset their hashed password using the argon2 used in the website
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(rounds=12))
    return hashed_password.decode('utf-8')


CONTROL_CHAR_RE = re.compile(r'[\x00-\x1F\x7F]')

def clean_text(s: str) -> str:
    """Remove all control characters from a string."""
    if not isinstance(s, str):
        return s
    return CONTROL_CHAR_RE.sub('', s)

def clean_dict(obj):
    """Recursively clean strings inside dicts/lists/tuples."""
    if isinstance(obj, dict):
        return {k: clean_dict(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_dict(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(clean_dict(v) for v in obj)
    elif isinstance(obj, str):
        return clean_text(obj)
    else:
        return obj
    

def get_domain_from_email(email: str) -> str:
    domain = email.split('@')[1]
    top_level_domain = get_fld(domain, fix_protocol=True, fail_silently=True)
    if top_level_domain is None:
        raise Exception(f"Could not detect First Level Domain in {email}")
    else:
        return top_level_domain
    
def get_domain_from_url(url: str) -> str:
    top_level_domain = get_fld(url, fix_protocol=True, fail_silently=True)
    if top_level_domain is None:
        raise Exception(f"Could not detect First Level Domain in {url}")
    else:
        return top_level_domain

def clean_date(dirty: str, format: str) -> datetime:
    format_to_regex = {
        "%Y": r"\d{4}",
        "%y": r"\d{2}",
        "%m": r"\d{1,2}",
        "%d": r"\d{1,2}",
        "%b": r"\w{3}",
        "%B": r"\w+"
    }
    pattern = format
    for k, v in format_to_regex.items():
        pattern = pattern.replace(k, v)

    match = re.search(pattern, dirty)
    if match:
        return datetime.strptime(match.group(), format)
    
    raise Exception("Date could not be parsed")