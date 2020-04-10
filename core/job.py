import dataclasses as dc
import typing as t


@dc.dataclass
class Job:
    backend_id: InterruptedError
    action:     str
    config:     t.Dict[str, t.Any]
