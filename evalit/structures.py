from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Union

TYPE_PATH = Union[str, Path]


@dataclass
class TransferDTO:
    # hold file name
    fname: str

    # holds which tool was used
    transferer: str

    # holds start/end time for the file transferred
    start_time: datetime = None

    # end_time: datetime = field(default_factory=lambda: datetime.now())
    end_time: datetime = None

    @property
    def transfer_time(self) -> float:
        return (self.end_time - self.start_time).seconds
