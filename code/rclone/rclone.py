#!/usr/bin/env python3

from __future__ import annotations

from typing import List

import os
import re
import subprocess
from dataclasses import dataclass

from loguru import logger


@dataclass
class ExecutionDTO:
    cmd: List[str]
    output: List[str]
    errors: List[str]
    status_code: int = 0

    @classmethod
    def default_empty_object(cls) -> ExecutionDTO:
        return cls(cmd=[], output=[], errors=[], status_code=-1)


class ShellExecutor:
    def __init__(self, stdout=subprocess.PIPE, stderr=subprocess.STDOUT):
        self.stdout = stdout
        self.stderr = stderr
        self.lines = []

    @property
    def dangerous_commands(self) -> List[str]:
        return ["rm", "rm *", "rm -rf"]

    def is_dangerous_command(self, cmd: List[str]) -> bool:
        cmd = " ".join(cmd)
        cmd = re.sub(r"\s+", " ", cmd).strip()
        logger.info(f"Executing command = {cmd}")
        return cmd in self.dangerous_commands

    def __call__(self, commands: List[str]):
        assert not self.is_dangerous_command(commands)

        exdto = ExecutionDTO(cmd=commands, output=[], errors=[])
        with subprocess.Popen(
            commands, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        ) as proc:
            (out, err) = proc.communicate()
            exdto.output = out.decode("utf-8")
            exdto.errros = err
        return exdto


class Rclone:
    def __init__(self, cfg: str) -> None:
        # path
        self.cfg = cfg

    def get_all_files(self, src: str, path: str) -> List[str]:
        cmd = [
            "rclone",
            "-v",
            "ls",
            f"{src}:{path}",
            f"--config={self.cfg}",
        ]
        exdto = ShellExecutor()(cmd)
        listings = exdto.output.split("\n")
        listings = list(filter(None, listings))
        if not listings:
            return []

        first = os.path.join(path, listings[0].split()[-1])
        listings = listings[1:]
        files = map(lambda l: " ".join(l.split()[1:]), listings)
        files = map(lambda f: os.path.join(path, f), files)
        files = list(files)
        logger.info(f"{len(files)} files found at {src}:{path}")
        return [first] + files


def main():
    rclone = Rclone("../test_conf.conf")
    files = rclone.get_all_files(src="s3-source-2", path="gael-test/")
    print(files[:5])
    with open("files.txt", "w") as f:
        f.write("\n".join(files[:10]))


if __name__ == "__main__":
    main()
