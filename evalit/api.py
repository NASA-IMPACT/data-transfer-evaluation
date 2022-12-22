# flake8: noqa

from .controller import StandardAutomationController
from .mft import MFTAutomation
from .nifi import NifiAutomation
from .rclone import RcloneAutomation
from .structures import TransferDTO


class BaseAPIs:
    from ._base import AbstractAutomation, AbstractController
