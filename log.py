from modules.object.log import Log, insert

def record_status(msg: str):
    item = Log('status', None, msg)
    insert(item)

def record_notice(msg: str):
    item = Log('notice', None, msg)
    insert(item)

def record_error(msg: str, code: str | int | None = None):
    item = Log('error', code, msg)
    insert(item)