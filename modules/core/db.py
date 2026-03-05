import os
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

load_dotenv()

class DatabasePoolSingleton:
    _instances = {}  # Stores instances by database name

    def __new__(cls, db_name):
        if db_name not in cls._instances:
            # Create the instance only if it doesn't exist for this DB name
            instance = super().__new__(cls)
            instance._init_pool(db_name)
            cls._instances[db_name] = instance
        return cls._instances[db_name]

    def _init_pool(self, db_name):
        self.db_name = db_name
        self._pool = None
        # Shared credentials from environment
        self.host = os.getenv('SECRET_DATABASE_HOST')
        self.port = os.getenv('SECRET_DATABASE_PORT')
        self.user = os.getenv('SECRET_DATABASE_USER')
        self.password = os.getenv('SECRET_DATABASE_PASSWORD')

    def get_pool(self):
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

    def get_connection(self):
        return self.get_pool().connection()
    
    def get_max_connections(self):
        return self.get_pool().max_size
    
    def close_all_connections(self):
        if self._pool:
            self._pool.close()
            self._pool = None
            print(f"Pool for {self.db_name} closed")

# Create/Retrieve the specific instances
db_pool_instance = DatabasePoolSingleton(os.getenv('SECRET_DATABASE_NAME'))
db_pool_instance_bt = DatabasePoolSingleton(os.getenv('SECRET_DATABASE_NAME_BT'))