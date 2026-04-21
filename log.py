from modules.object.log import Log, insert

def record_status(msg: str) -> None:
    item = Log(log_type='status', code=None, msg=msg)
    insert(item)

def record_notice(msg: str) -> None:
    item = Log(log_type='notice', code=None, msg=msg)
    insert(item)

def record_error(msg: str, code: str | int | None = None) -> None:
    item = Log(log_type='error', code=code, msg=msg)
    insert(item)