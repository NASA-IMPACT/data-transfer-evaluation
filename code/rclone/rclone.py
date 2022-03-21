#!/usr/bin/env python3

from __future__ import annotations

from typing import List, Optional

import copy
import os
import random
import re
import subprocess
import tempfile

from dataclasses import dataclass

from loguru import logger


@dataclass
class ExecutionDTO:
    """
    Holds Shel execution data
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
            exdto.output = out.decode("utf-8").split("\n")
            exdto.errros = err
        return exdto


class Rclone:
    """
    rclone wrapper to be used in python
    """

    def __init__(
        self, cfg: str, logfile: Optional[str] = None, verbosity: str = "-v"
    ) -> None:
        # path
        self.cfg = cfg
        self.logfile = logfile
        self.verbosity = verbosity or "-v"

    def get_all_files(self, src: str, path: str) -> List[str]:
        cmd = [
            "rclone",
            f"{self.verbosity}",
            "ls",
            f"{src}:{path}",
            f"--config={self.cfg}",
        ]
        exdto = ShellExecutor()(cmd)
        listings = exdto.output
        listings = list(filter(None, listings))
        if not listings:
            return []

        # first line has a different listing pattern
        first = os.path.join(path, listings[0].split()[-1])

        # last line is not a path
        listings = listings[1:-1]

        # first word is number of bytes, not needed for now
        files = map(lambda l: " ".join(l.split()[1:]), listings)
        files = map(lambda f: os.path.join(path, f), files)
        files = list(files)
        logger.info(f"{len(files)} files found at {src}:{path}")
        return [first] + files

    def copy_files(
        self,
        src: str,
        dest: str,
        path: str,
        files: List[str],
        ntransfers: int = 8,
        buffer_size: int = 512,
        multi_thread_streams: int = 5,
        multi_thread_cutoff: int = 50,
        update: bool = True,
        randomize: bool = False,
        debug: bool = False,
    ):
        """
        Args:
            src: ``str``
                Source name residing in the config file
            dest: ``str``
                Destination name residing in the config file
            path: ``str``
                Destination path.
                Used in conjoint with ``dest`` as `dest:path`
            files: ``List[str]``
                List of paths in the source `src`.
            ntransfers: ``int``
                Concurrency.
            buffer_size: ``int``
                Memory size in MegaByte (MB) to allocate for each file
            multi_thread_streams: ``int``
                How many chunks to be used for each file for multi-stream transfer?
            multi_thread_cutoff: ``int``
                What's the minimum number of file size (in MB) to break into multiple streams?
                That is: a file will be broken into ``multi_thread_streams`` if its size
                exceeds ``multi_thread_cutoff``
            update: ``bool``
                If this flag is set we add ``--update`` flag to rclone command.
                If set, rclone won't copy files that are already in the destination.
            randomize: ``bool``
                If  set, we first shuffle the ``files`` list.

        Note:
            - ``multi_thread_cutoff`` and ``multi_thread_streams`` are more
            like hyperparameter (tunable).
            - high ``multi_thread_streams`` and low ``multi_thread_cutoff`` can
            significantly degrade the performance as it will have more network overhead.
        """
        logger.info(f"Transferring {len(files)} files from {src} to {dest}:{path}")

        if randomize:
            files = copy.deepcopy(files)
            random.shuffle(files)

        if debug:
            logger.debug(f"Files ==> {files}")

        exdto = ExecutionDTO.default_empty_object()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".paths") as ftemp:
            ftemp.write("\n".join(files))
            ftemp.flush()
            logger.debug(f"Using pathfile = {ftemp.name}")
            cmd = [
                "rclone",
                f"{self.verbosity}",
                "copy",
                f"--files-from={ftemp.name}",
                f"{src}:",
                f"{dest}:{path}",
                f"--config={self.cfg}",
                f"--log-file={self.logfile}",
                f"--transfers={ntransfers}",
                f"--buffer-size={buffer_size}M",
                f"--multi-thread-streams={multi_thread_streams}",
                f"--multi-thread-cutoff={multi_thread_cutoff}M",
                "--progress",
            ] + (["--update"] if update else [])
            exdto = ShellExecutor()(cmd)

        # with open(self.logfile, "a") as flog:
        #     flog.write("=" * 7)
        #     flog.write("\n".join(exdto.output))
        #     flog.write("=" * 7)
        return exdto


def main():
    rclone = Rclone(cfg="rclone.conf", logfile="tmp/log.txt", verbosity="-v")
    files = rclone.get_all_files(src="s3-source-2", path="gael-test/")
    _ = rclone.copy_files(
        src="s3-source-2",
        dest="s3-target-2",
        path="evaluation-transfers-bucket-testing-only",
        # files=files[:10],
        files=files[-1:-7:-1],
        buffer_size=1024,
        multi_thread_streams=5,
        multi_thread_cutoff=25,
        randomize=True,
        update=False,
        debug=True,
    )
    # print(files[:5])
    # with open("files.txt", "w") as f:
    #     f.write("\n".join(files[:10]))


if __name__ == "__main__":
    main()
