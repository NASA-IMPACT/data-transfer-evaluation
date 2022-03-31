import multiprocessing
import os
import time
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

        return transfer_id

    def run_automation(self, njobs: int = 4) -> Tuple[TransferDTO]:
        source_storage_id = self.config.get("source_storage_id", None)
        dest_storage_id = self.config.get("dest_storage_id", None)
        logger.debug(f"Source storage id = {source_storage_id}")
        logger.debug(f"Dest storage id = {dest_storage_id}")

        assert (
            source_storage_id and dest_storage_id
        ), "Invalid storage ids! Are you sure you have 'source_storage_id' and 'dest_storage_id' in the config?"

        njobs = njobs or multiprocessing.cpu_count()
        njobs = max(njobs, 1)
        logger.debug(f"njobs = {njobs}")

        transfer_ids = Parallel(n_jobs=njobs)(
            delayed(self.submit_transfer)(fname, source_storage_id, dest_storage_id)
            for fname in self.files
        )
        return self.parse_log(transfer_ids=transfer_ids, poll_wait_time=5)

    def parse_log(
        self, transfer_ids: List[str], poll_wait_time: int = 5
    ) -> Tuple[TransferDTO]:
        nids = len(transfer_ids)

        timekeeper = {}
        end_counter = 0
        while (len(timekeeper) < nids) and (end_counter < nids):
            # Note: Why?
            time.sleep(poll_wait_time)

            for transfer_id in transfer_ids:
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
                            transfer_id,
                            TransferDTO(fname=transfer_id, transferer="mft"),
                        )
                        dto.start_time = int(part.split("|")[1].strip()) / 1000
                        timekeeper[transfer_id] = dto

                    if part.find("COMPLETED") != -1:
                        dto = timekeeper.get(
                            transfer_id,
                            TransferDTO(fname=transfer_id, transferer="mft"),
                        )
                        dto.end_time = int(part.split("|")[1].strip()) / 1000
                        timekeeper[transfer_id] = dto
                        end_counter += 1
        return tuple(timekeeper.values())
