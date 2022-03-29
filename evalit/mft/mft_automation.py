import multiprocessing
import os
import time
from typing import Dict, Optional, Sequence, Union

from joblib import Parallel, delayed
from loguru import logger

from .._base import AbstractAutomation
from ..misc.shell import ShellExecutor
from ..structures import TYPE_PATH


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

    def run_automation(self, njobs: int = 4):
        source_storage_id = self.config.get("source_storage_id", None)
        dest_storage_id = self.config.get("dest_storage_id", None)
        assert source_storage_id and dest_storage_id, "Invalid storage ids!"

        njobs = njobs or multiprocessing.cpu_count()
        njobs = max(njobs, 1)
        logger.debug(f"njobs = {njobs}")

        transfer_ids = Parallel(n_jobs=njobs)(
            delayed(self.submit_transfer)(fname, source_storage_id, dest_storage_id)
            for fname in self.files
        )

        start_time_map = {}
        end_time_map = {}

        # TODO: Optimize this!
        while 1:
            # Note: Why?
            time.sleep(5)

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
                        start_time = part.split("|")[1].strip()
                        start_time_map[transfer_id] = int(start_time) / 1000
                        logger.debug("Start time: ", start_time)

                    if part.find("COMPLETED") != -1:
                        end_time = part.split("|")[1].strip()
                        end_time_map[transfer_id] = int(end_time) / 1000
                        logger.debug("End time: ", end_time)

            if len(start_time_map) == len(transfer_ids) and len(end_time_map) == len(
                transfer_ids
            ):
                break

        final_time_array = []

        for key, start_time in start_time_map.items():
            end_time = end_time_map[key]
            print(key, end_time - start_time)
            final_time_array.append([start_time, end_time])

        return final_time_array
