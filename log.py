from modules.object.log import Log, insert

def record_status(msg: str) -> None:
    item = Log('status', None, msg)
    insert(item)

def record_notice(msg: str) -> None:
    item = Log('notice', None, msg)
    insert(item)

def record_error(msg: str, code: str | int | None = None) -> None:
    item = Log('error', code, msg)
    insert(item)