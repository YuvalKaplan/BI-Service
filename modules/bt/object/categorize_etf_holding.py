from typing import Optional, List
from datetime import date, datetime
from psycopg.errors import Error
from psycopg.rows import class_row
from dataclasses import dataclass
from modules.core.db import db_pool_instance
import pandas as pd

@dataclass
class CategorizeEtfHolding:
    categorize_etf_id: int
    holding_date: date
    ticker: str
    id: Optional[int]
    created_at: Optional[datetime]

