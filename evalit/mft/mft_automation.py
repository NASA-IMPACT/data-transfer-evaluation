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
    """

    def __init__(
        self,
        config: Union[Dict[str, str], TYPE_PATH],
        mft_dir: TYPE_PATH,
        files: Optional[Sequence[TYPE_PATH]] = None,
        shell_executor: Optional[ShellExecutor] = None,
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

    def submit_transfer(
        self, file_name: str, source_storage_id: str, dest_storage_id: str
    ):
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
        logger.info(f"Fetched Trasnfer id = {transfer_id}")

        return (transfer_id, file_name)

    def run_automation(self, **kwargs) -> Tuple[TransferDTO]:

        assert (
            self.source_storage_id and self.dest_storage_id
        ), "Invalid storage ids! Are you sure you have 'source_storage_id' and 'dest_storage_id' in the config?"

        cpu_count = multiprocessing.cpu_count()
        njobs = kwargs.get("mft_njobs", cpu_count) or cpu_count
        # clip to >=1
        njobs = max(1, njobs)
        njobs = min(njobs, cpu_count)
        logger.debug(f"njobs = {njobs}")

        transfer_id_names = Parallel(n_jobs=njobs)(
            delayed(self.submit_transfer)(
                fname, self.source_storage_id, self.dest_storage_id
            )
            for fname in self.files
        )
        return self.parse_log(transfer_id_names=transfer_id_names, poll_wait_time=5)

    def parse_log(
        self, transfer_id_names: List[str], poll_wait_time: int = 5
    ) -> Tuple[TransferDTO]:
        nids = len(transfer_id_names)

        timekeeper = {}
        end_counter = 0
        while end_counter < nids:
            logger.debug(
                f"[{self.__classname__}] log parser polling... {end_counter}/{nids} files transferred!"
            )
            time.sleep(poll_wait_time)

            end_counter = 0
            for (transfer_id, file_name) in transfer_id_names:
                cmd = [
                    "java",
                    "-jar",
                    os.path.join(self.mft_dir, "mft-client.jar"),
                    "transfer",
                    "state",
                    "-a",
                    transfer_id,
                ]
                exdto = self.shell_executor(cmd)

                for part in exdto.output:
                    if part.find("STARTING") != -1:
                        dto = timekeeper.get(
                            file_name,
                            TransferDTO(fname=file_name, transferer="mft"),
                        )
                        dto.start_time = datetime.fromtimestamp(
                            int(part.split("|")[1].strip()) / 1000
                        )
                        timekeeper[file_name] = dto

                    if part.find("COMPLETED") != -1:
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
