from datetime import datetime
from dataclasses import dataclass
from modules.core.util import get_file_hash

@dataclass
class File:
    format: str
    filename: str
    local_path: str
    size: int
    data: bytes | str
    hash: str
    date: datetime | None

    def __init__(self, filename, format: str, data: bytes | str, date: datetime | None = None):
        self.filename = filename
        self.data = data
        self.date = date
        self.format = format
        self.size = len(data)
        if (format == 'pdf' or format == 'eml') and isinstance(data, bytes):
            self.hash = get_file_hash(data)        
        elif format == 'html' and isinstance(data, str):
            self.hash = get_file_hash(data.encode("utf-8"))
        else:
            raise Exception(f"File {filename} data does not match the file type {format}")     

    def get_bytes(self) -> bytes | None:
        if (self.format == 'pdf' or self.format == 'eml') and isinstance(self.data, bytes):
            return self.data
        elif self.format == 'html' and isinstance(self.data, str):
            return self.data.encode("utf-8")
        return None
