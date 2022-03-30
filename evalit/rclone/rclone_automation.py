import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Sequence, Union

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from loguru import logger

from .._base import AbstractAutomation
from ..misc.shell import ShellExecutor
from ..structures import TYPE_PATH


class RcloneAutomation(AbstractAutomation):
    def __init__(
        self,
        config: Union[Dict[str, str], TYPE_PATH],
        files: Optional[Sequence[TYPE_PATH]] = None,
        shell_executor: Optional[ShellExecutor] = None,
        debug: bool = False,
        **params,
    ) -> None:
        super().__init__(config=config, files=files, debug=debug)

        shell_executor = shell_executor or ShellExecutor()
        assert isinstance(shell_executor, ShellExecutor)
        self.shell_executor = shell_executor

        logger.debug(params)
        self.buffer_size = params.get("buffer_size", 50)
        self.multi_thread_streams = params.get("multi_thread_streams", 10)
        self.multi_thread_cutoff = params.get("multi_thread_cutoff", 50)
        self.ntransfers = params.get("ntransfers", 8)
        self.s3_max_upload_parts = params.get("s3_max_upload_parts", 10)
        self.s3_upload_concurrency = params.get("s3_upload_concurrency", 10)

    def _generate_rclone_cfg(self) -> tempfile.NamedTemporaryFile:
        source_token = self.config["source_token"]
        source_secret = self.config["source_secret"]
        source_s3_endpoint = self.config["source_s3_endpoint"]

        dest_token = self.config["dest_token"]
        dest_secret = self.config["dest_secret"]
        dest_s3_endpoint = self.config["dest_s3_endpoint"]

        # segregate this to another method
        lines = [
            "[s3source]\n",
            "type = s3\n",
            "provider = Other\n",
            f"access_key_id = {source_token}\n",
            f"secret_access_key = {source_secret}\n",
            f"endpoint = {source_s3_endpoint}\n",
            "acl = authenticated-read\n",
            "\n",
            "[s3dest]\n",
            "type = s3\n",
            "provider = Other\n",
            f"access_key_id = {dest_token}\n",
            f"secret_access_key = {dest_secret}\n",
            f"endpoint = {dest_s3_endpoint}\n",
            "acl = authenticated-read\n" "\n",
        ]

        # We don't use context manager here as it will auto-delete the file
        # Or we have to manually delete the file even after closing the
        # file pointer
        ftemp = tempfile.NamedTemporaryFile(mode="w", suffix=".conf", prefix="rclone_")
        ftemp.writelines(lines)
        ftemp.flush()
        return ftemp

    def run_automation(self):
        source_s3_bucket = self.config["source_s3_bucket"]
        source_s3_region = self.config["source_s3_region"]
        dest_s3_bucket = self.config["dest_s3_bucket"]
        dest_s3_region = self.config["dest_s3_region"]

        total_files = len(self.files)

        # temp files
        rclone_log_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="rclone_", suffix=".log", delete=False
        )
        logger.debug(f"rclone log file :: {rclone_log_file.name}")

        rclone_config_file = self._generate_rclone_cfg()
        assert rclone_config_file is not None
        logger.debug(f"rclone conf file :: {rclone_config_file.name}")

        cmd = [
            "rclone",
            "copy",
            f"s3source:{source_s3_bucket}",
            f"s3dest:{dest_s3_bucket}",
            f"--multi-thread-streams={self.multi_thread_streams}",
            f"--multi-thread-cutoff={self.multi_thread_cutoff}M",
            f"--s3-max-upload-parts={self.s3_max_upload_parts}",
            f"--s3-upload-concurrency={self.s3_upload_concurrency}",
            f"--buffer-size={self.buffer_size}M",
            f"--transfers={self.ntransfers}",
            "--progress",
            f"--config={rclone_config_file.name}",
            f"--log-file={rclone_log_file.name}",
            "--log-level=DEBUG",
            "-I",
        ]

        start = time.time()
        exdto = self.shell_executor(cmd)
        logger.debug(f"Execution took {time.time()-start} seconds.")

        # this deletes the temp file also
        logger.info(f"Removing temp config at {rclone_config_file.name}")
        rclone_config_file.close()

        start_time_map = {}
        end_time_map = {}

        rclone_log_file.seek(0)
        for line in rclone_log_file:
            if (
                line.find("multipart upload starting chunk 1 size") != -1
                or line.find("Transferring unconditionally") != -1
            ):
                if self.debug:
                    logger.debug(
                        line.split(":")[3].strip(), line.split("DEBUG")[0].strip()
                    )
                utc_time = datetime.strptime(
                    line.split("DEBUG")[0].strip(), "%Y/%m/%d %H:%M:%S"
                )
                start_time_map[line.split(":")[3].strip()] = utc_time

            if line.find("Copied") != -1:
                if self.debug:
                    logger.debug(
                        line.split(":")[3].strip(), line.split("INFO")[0].strip()
                    )
                utc_time = datetime.strptime(
                    line.split("INFO")[0].strip(), "%Y/%m/%d %H:%M:%S"
                )
                end_time_map[line.split(":")[3].strip()] = utc_time

        final_time_array = []

        for key, start_time in start_time_map.items():
            end_time = end_time_map[key]
            logger.debug(f"{key}, {(end_time - start_time).total_seconds()}")
            final_time_array.append(
                [
                    (start_time - datetime(1970, 1, 1)).total_seconds(),
                    (end_time - datetime(1970, 1, 1)).total_seconds(),
                ]
            )

        return final_time_array
