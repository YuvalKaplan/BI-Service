import os
from psycopg_pool import ConnectionPool

class DatabasePoolSingleton:
    _pool = None

    def get_pool(self):
        if self._pool is None:
            # Initialize the connection pool only once
            # Environment variables from .env file
            from dotenv import load_dotenv
            load_dotenv()
            print(f"Connecting DB pool to: host={os.getenv('SECRET_DATABASE_HOST')} port={os.getenv('SECRET_DATABASE_PORT')} dbname={os.getenv('SECRET_DATABASE_NAME')} user={os.getenv('SECRET_DATABASE_USER')}")
            conninfo = f"host={os.getenv('SECRET_DATABASE_HOST')} port={os.getenv('SECRET_DATABASE_PORT')} dbname={os.getenv('SECRET_DATABASE_NAME')} user={os.getenv('SECRET_DATABASE_USER')} password={os.getenv('SECRET_DATABASE_PASSWORD')}"
            self._pool = ConnectionPool(conninfo, min_size=1, max_size=10)
            print('Connection pool available')
        return self._pool

    def get_connection(self):
        return self.get_pool().connection() 

    def get_max_connections(self):
        return self.get_pool().max_size

    def close_all_connections(self):
        if self._pool:
            self._pool.close()
            self._pool = None
            print('Connection pool closed')
    
 # Create the single instance of MySingletonClass
db_pool_instance = DatabasePoolSingleton()
