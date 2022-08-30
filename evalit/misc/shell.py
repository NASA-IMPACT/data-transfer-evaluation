from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from typing import List, Optional

from loguru import logger


@dataclass
class ExecutionDTO:
    """
    Holds Shell execution data
    """

    cmd: List[str]
    output: List[str]
    errors: List[str]
    status_code: Optional[int] = None

    @classmethod
    def default_empty_object(cls) -> ExecutionDTO:
        return cls(cmd=[], output=[], errors=[], status_code=-1)


class ShellExecutor:
    """
    A barebone interface to execute shell commands...
    """

    def __init__(self, stdout=subprocess.PIPE, stderr=subprocess.STDOUT):
        self.stdout = stdout
        self.stderr = stderr

    @property
    def dangerous_commands(self) -> List[str]:
        return ["rm", "rm *", "rm -rf"]

    def is_dangerous_command(self, cmd: List[str]) -> bool:
        cmd = tuple(filter(None, cmd))
        cmd_str = " ".join(cmd).strip()
        cmd_str = re.sub(r"\s+", " ", cmd_str).strip()
        return cmd in self.dangerous_commands or cmd[0] in self.dangerous_commands

    def __call__(self, commands: List[str]):
        logger.info(f"Executing command = {commands}")
        assert not self.is_dangerous_command(commands)

        exdto = ExecutionDTO(cmd=commands, output=[], errors=[])
        with subprocess.Popen(commands,
                              shell=False,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT
        ) as proc:
            (out, err) = proc.communicate()
            exdto.output = out.decode("utf-8").split("\n")
            exdto.errros = err
        return exdto
