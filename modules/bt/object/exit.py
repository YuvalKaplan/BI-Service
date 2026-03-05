from modules.core.db import db_pool_instance_bt

def cleanup():
    db_pool_instance_bt.close_all_connections()
