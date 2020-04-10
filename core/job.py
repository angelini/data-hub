import dataclasses as dc
import typing as t


@dc.dataclass
class Job:
    backend_id: str
    action:     str
    config:     t.Dict[str, t.Any]
