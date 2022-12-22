import multiprocessing
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple, Union

from joblib import Parallel, delayed
from loguru import logger

from .._base import AbstractAutomation
from ..misc.shell import ShellExecutor
from ..structures import TYPE_PATH, TransferDTO


class MFTAutomation(AbstractAutomation):
    """
    This is the data transfer automation component for MFT.
    This acts as an interface to `nifi` server.
    (See installation/setup guide for nifi)

    Args:

        ```config```: ```Union[str, pathlib.Path, Dict[str, str]]```
                Configuration for the transfer repo

        ```mft_dir```: ```Union[str, pathlib.Path]```
                Path to the mft installation directory
                (Normally, we get this from the environment variable `MFT_INSTALLATION`)

        ```files```: ```List[str]```
            List of filenames to be transferred.
            (Currently it's not used in RcloneAutomation)

        ```shell_executor```: ```misc.shell.ShellExecutor```
            A misc component to execute external shell commands.

        ```njobs```: ```int```
            Number of parallel jobs to be used for mft job submission
            and log parser

        ```debug```: ```bool```
            Debug mode flag. Defaults to False.

    """

    # these are used for parsing nifi log
    _LOG_START_PHRASE = "STARTING"
    _LOG_COMPLETE_PHRASE = "COMPLETED"

    def __init__(
        self,
        config: Union[Dict[str, str], TYPE_PATH],
        mft_dir: TYPE_PATH,
        files: Optional[Sequence[TYPE_PATH]] = None,
        shell_executor: Optional[ShellExecutor] = None,
        njobs: int = 4,
        debug: bool = False,
    ):
        super().__init__(config=config, files=files, debug=debug)

        assert os.path.exists(mft_dir), f"{mft_dir} path doesn't exist!"
        self.mft_dir = mft_dir

        shell_executor = shell_executor or ShellExecutor()
        assert isinstance(shell_executor, ShellExecutor)
        self.shell_executor = shell_executor

        cmd = [
            "java",
            "-jar",
            os.path.join(self.mft_dir, "mft-client.jar"),
            "s3",
            "remote",
            "add",
            "-b",
            config["source_s3_bucket"],
            "-e",
            config["source_s3_endpoint"],
            "-k",
            config["source_token"],
            "-s",
            config["source_secret"],
            "-n",
            "sources3",
            "-r",
            config["source_s3_region"],
        ]
        exdto = self.shell_executor(cmd)

        self.source_storage_id = None
        self.dest_storage_id = None

        for source_s3_out in exdto.output:
            if source_s3_out.startswith("Storage Id"):
                self.source_storage_id = source_s3_out.split(" ")[2].strip()
                logger.debug(f"Source storage id = {self.source_storage_id}")
                break

        cmd = [
            "java",
            "-jar",
            os.path.join(self.mft_dir, "mft-client.jar"),
            "s3",
            "remote",
            "add",
            "-b",
            config["dest_s3_bucket"],
            "-e",
            config["dest_s3_endpoint"],
            "-k",
            config["dest_token"],
            "-s",
            config["dest_secret"],
            "-n",
            "dests3",
            "-r",
            config["dest_s3_region"],
        ]
        exdto = self.shell_executor(cmd)

        for dest_s3_out in exdto.output:
            if dest_s3_out.startswith("Storage Id"):
                self.dest_storage_id = dest_s3_out.split(" ")[2].strip()
                logger.debug(f"Destination storage id = {self.dest_storage_id}")
                break

        cpu_count = multiprocessing.cpu_count()
        njobs = njobs or cpu_count
        # clip to >=1
        njobs = max(1, njobs)
        njobs = min(njobs, cpu_count)
        self.njobs = njobs
        logger.debug(f"njobs = {njobs}")

    def submit_transfer(
        self, file_name: str, source_storage_id: str, dest_storage_id: str
    ) -> Tuple[str, str]:
        """
        Submit file for transfer through mft agent.

        Args:
            ```file_name```: ```str```
                Name of the file to submit to mft for transfer
            ```source_storage_id```: ```str```
                Storage id for souce bucket
            ```dest_storage_id```: ```str```
                Storage id for destination bucket

        Returns:
            `Tuple[str, str]` tuple of transsfer id and filename.
        """
        cmd = [
            "java",
            "-jar",
            self.mft_dir + "/mft-client.jar",
            "transfer",
            "submit",
            "-d",
            dest_storage_id,
            "-s",
            source_storage_id,
            "-sp",
            file_name,
            "-dp",
            file_name,
            "-st",
            "S3",
            "-dt",
            "S3",
        ]

        exdto = self.shell_executor(cmd)

        # Notes from Nish:
        # to maintain the same previous logic, I just created the original output string
        stdout = "\n".join(exdto.output)
        transfer_id = stdout.split("Submitted Transfer ")[1].strip()
        if self.debug:
            logger.debug(f"Fetched Trasnfer id = {transfer_id}")

        return (transfer_id, file_name)

    def run_automation(self, **kwargs) -> Tuple[TransferDTO]:
        """
        Runs mft automation.

        Args:
            ```kwargs```: ```Any```
                extra params to be used for the transfer.
                - `mft_log_poll_time`
                    - every N seconds check whether full transfer is done.

        Returns:
            Tuple of individual file data transfer `Tuple[TransferDTO]`,
            where each element object stores
            - filename
            - start_time
            - end_time
            - transferer ("rclone")
        """
        assert (
            self.source_storage_id and self.dest_storage_id
        ), "Invalid storage ids! Are you sure you have 'source_storage_id' and 'dest_storage_id' in the config?"

        logger.debug(f"njobs = {self.njobs}")
        transfer_id_names = Parallel(n_jobs=self.njobs)(
            delayed(self.submit_transfer)(
                fname, self.source_storage_id, self.dest_storage_id
            )
            for fname in self.files
        )
        return self.parse_log(
            transfer_id_names=transfer_id_names,
            poll_wait_time=kwargs.get("mft_log_poll_time", 5) or 5,
            njobs=kwargs.get("mft_log_parser_njobs", multiprocessing.cpu_count()) or 1,
        )

    def parse_log(
        self,
        transfer_id_names: List[str],
        poll_wait_time: int = 5,
        njobs: int = 1,
    ) -> Tuple[TransferDTO]:
        """
        Parses the log and returns start/end times for each file transfer.

        Args:
            ```transfer_id_names```: ```List[str]```
                List of files that are submitted for transfer
            ```poll_wait_time```: ```int```
                Every `poll_wait_time` seconds, check the status of transfer.
            ```njobs```: ```int```
                Total number of parallellism used for checking the status of
                transfer.

        Returns:
            Tuple of individual file data transfer `Tuple[TransferDTO]`,
            where each element object stores
            - filename
            - start_time
            - end_time
            - transferer ("nifi")
        """
        nids = len(transfer_id_names)

        timekeeper = {}
        end_counter = 0
        while end_counter < nids:
            logger.debug(
                f"[{self.__classname__}] log parser polling... {end_counter}/{nids} files transferred!"
            )
            time.sleep(poll_wait_time)
            end_counter = 0

            # get log outputs for all the transfer ids
            outputs = Parallel(n_jobs=njobs)(
                delayed(self.shell_executor)(
                    [
                        "java",
                        "-jar",
                        os.path.join(self.mft_dir, "mft-client.jar"),
                        "transfer",
                        "state",
                        "-a",
                        transfer_id,
                    ]
                )
                for (transfer_id, file_name) in transfer_id_names
            )

            # parse each log output
            for exdto, (transfer_id, file_name) in zip(outputs, transfer_id_names):
                for part in exdto.output:
                    if part.find(self._LOG_START_PHRASE) != -1:
                        dto = timekeeper.get(
                            file_name,
                            TransferDTO(fname=file_name, transferer="mft"),
                        )
                        dto.start_time = datetime.fromtimestamp(
                            int(part.split("|")[1].strip()) / 1000
                        )
                        timekeeper[file_name] = dto

                    if part.find(self._LOG_COMPLETE_PHRASE) != -1:
                        dto = timekeeper.get(
                            file_name,
                            TransferDTO(fname=file_name, transferer="mft"),
                        )
                        dto.end_time = datetime.fromtimestamp(
                            int(part.split("|")[1].strip()) / 1000
                        )
                        timekeeper[file_name] = dto
                        end_counter += 1

        return tuple(timekeeper.values())
