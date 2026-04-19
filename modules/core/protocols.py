from typing import Protocol


class CategorizeEtfProtocol(Protocol):
    id: int | None
    url: str | None
    name: str | None
    file_format: str | None
    mapping: dict | None
    trigger_download: dict | None
    wait_pre_events: str | None
    wait_post_events: str | None
    events: dict | None
