import os
import sys
from contextlib import AbstractContextManager
from psycopg import Connection
from psycopg.rows import TupleRow
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

load_dotenv()

def _resolve_environment() -> str:
    if '--prod' in sys.argv:
        return 'production'
    if '--dev' in sys.argv:
        return 'development'
    return 'production' if os.getenv('ENV_TYPE') == 'production' else 'development'

ENVIRONMENT = _resolve_environment()
print(f"[db] Resolved database environment: {ENVIRONMENT}")

class DatabasePoolSingleton:
    _instances = {}  # Stores instances by (database name, use_prod)

    def __new__(cls, db_name, use_prod: bool = False):
        key = (db_name, use_prod)
        if key not in cls._instances:
            # Create the instance only if it doesn't exist for this key
            instance = super().__new__(cls)
            instance._init_pool(db_name, use_prod)
            cls._instances[key] = instance
        return cls._instances[key]

    def _init_pool(self, db_name: str, use_prod: bool) -> None:
        self.db_name = db_name
        self._pool: ConnectionPool[Connection[TupleRow]] | None = None
        # Shared credentials from environment
        prefix = 'SECRET_DATABASE_PROD_' if use_prod else 'SECRET_DATABASE_'
        self.host = os.getenv(f'{prefix}HOST')
        self.port = os.getenv(f'{prefix}PORT')
        self.user = os.getenv(f'{prefix}USER')
        self.password = os.getenv(f'{prefix}PASSWORD')

    def get_pool(self) -> ConnectionPool[Connection[TupleRow]]:
        if self._pool is None:
            conninfo = (
                f"host={self.host} port={self.port} "
                f"dbname={self.db_name} user={self.user} "
                f"password={self.password}"
            )
            print(f"Connecting pool to: {self.db_name}")
            # Initialize with your specific min/max sizes
            self._pool = ConnectionPool(conninfo, min_size=1, max_size=10)
        return self._pool

    def get_connection(self) -> AbstractContextManager[Connection[TupleRow]]:
        return self.get_pool().connection()

    def get_max_connections(self) -> int:
        return self.get_pool().max_size

    def close_all_connections(self) -> None:
        if self._pool:
            self._pool.close()
            self._pool = None
            print(f"Pool for {self.db_name} closed")

# Create/Retrieve the specific instances
_USE_PROD = ENVIRONMENT == 'production'
db_pool_instance = DatabasePoolSingleton(
    os.getenv('SECRET_DATABASE_PROD_NAME') if _USE_PROD else os.getenv('SECRET_DATABASE_NAME'),
    use_prod=_USE_PROD,
)
# Backtesting always uses the development database, regardless of --prod/--dev
db_pool_instance_bt = DatabasePoolSingleton(os.getenv('SECRET_DATABASE_NAME_BT'), use_prod=False)