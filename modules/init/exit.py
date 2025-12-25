from modules.core.db import db_pool_instance

def cleanup():
    db_pool_instance.close_all_connections()
