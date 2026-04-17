from modules.core.db import db_pool_instance

def cleanup() -> None:
    db_pool_instance.close_all_connections()
