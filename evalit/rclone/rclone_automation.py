import tempfile
import time
from datetime import datetime
from typing import Dict, Optional, Sequence, TextIO, Tuple, Union

import urllib3
from loguru import logger

from .._base import AbstractAutomation
from ..misc.shell import ShellExecutor
from ..structures import TYPE_PATH, TransferDTO

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class RcloneAutomation(AbstractAutomation):
    """
    This is a s3-s3  transfer component using rclone.
    This acts as an interface to direct `rclone` command.

    Args:
        ```config```: ```Union[str, pathlib.Path, Dict[str, str]]```
            Represents  configuration for the framework.
            See `AbstractAutomation` documentation for more information.

        ```files```: ```List[str]```
            List of filenames to be transferred.
            (Currently it's not used in RcloneAutomation)

        ```shell_executor```: ```misc.shell.ShellExecutor```
            A misc component to execute external shell commands.

        ```debug```: ```bool```
            Flag for debugging mode.

        ```params```: ```Any```
            kwargs for more control for rcloen transfers.
            (See `Extra params` section)

    Extra params:
        For extra params to the rclone executor, we pass them as keyword args
        through `**params`. Some of the params are:

            - `buffer_size` (size of buffer)
            - `multi_thread_streams` (how many chunks is to be created for each
            file?)
            - `multi_thread_cutoff` (threshold in MB to start chunking a single
            file into multi_thread_streams chunks)
            - `ntransfers` (number of parallelization for downloads)
            - `s3_max_upload_parts` (how many chunks at max used to upload to s3?)
            - `s3_upload_concurrency` (number of parallelization for uploads)

    See: https://rclone.org/docs/
    """

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
        """
        Generate a temporary config file compatible to rclone.

        Returns:
            `tempfile.NamedTemporaryFile` file object.
        """
        source_token = self.config["source_token"]
        source_secret = self.config["source_secret"]
        source_s3_endpoint = self.config["source_s3_endpoint"]

        dest_token = self.config["dest_token"]
        dest_secret = self.config["dest_secret"]
        dest_s3_endpoint = self.config["dest_s3_endpoint"]

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

    def run_automation(self, **kwargs) -> Tuple[TransferDTO]:
        """
        Main entrypoint/interface to run the RcloneAutomation.

        Args:
            ```kwargs```: ```any```
                keyword args
                (for now, nothing is used!)

        Returns:
            Tuple of individual file data transfer `Tuple[TransferDTO]`,
            where each element object stores
            - filename
            - start_time
            - end_time
            - transferer ("rclone")
        """
        start_automation = time.time()
        source_s3_bucket = self.config["source_s3_bucket"]
        # source_s3_region = self.config["source_s3_region"]
        dest_s3_bucket = self.config["dest_s3_bucket"]
        # dest_s3_region = self.config["dest_s3_region"]

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
        _ = self.shell_executor(cmd)
        logger.debug(f"Execution took {time.time()-start} seconds.")

        # this deletes the temp file also
        logger.info(f"Removing temp config at {rclone_config_file.name}")
        rclone_config_file.close()

        # start_time_map, end_time_map = self.parse_log(rclone_log_file, debug=self.debug)
        vals = self.parse_log(rclone_log_file, debug=self.debug)
        logger.debug(
            f"Delta time for {self.__classname__} = {time.time() - start_automation}"
        )
        return vals

    def parse_log(
        self, log: Union[str, TextIO], debug: bool = False
    ) -> Tuple[TransferDTO]:
        """
        Parse rclone-generated log file to extract transfer information
        for each file.

        Args:
            ```log```: ```Union[str, TextIO]```
                Rclone log file to be parsed
            ```debug```: ```bool```
                Debugging mode flag

        Returns:
            tuple of data transfer metadata `Tuple[TransferDTO]`.
        """
        # in case it's a path
        if isinstance(log, str):
            log = open(log)
        log.seek(0)

        logger.debug(f"Parsing log at {log.name}")

        timekeeper = {}
        for line in log:
            if (
                line.find("multipart upload starting chunk 1 size") != -1
                or line.find("Transferring unconditionally") != -1
            ):
                fname = line.split(":")[3].strip()
                dto = timekeeper.get(
                    fname, TransferDTO(fname=fname, transferer="rclone")
                )
                dto.start_time = datetime.strptime(
                    line.split("DEBUG")[0].strip(), "%Y/%m/%d %H:%M:%S"
                )
                timekeeper[fname] = dto

            if line.find("Copied") != -1:
                fname = line.split(":")[3].strip()
                dto = timekeeper.get(
                    fname, TransferDTO(fname=fname, transferer="rclone")
                )
                dto.end_time = datetime.strptime(
                    line.split("INFO")[0].strip(), "%Y/%m/%d %H:%M:%S"
                )
                timekeeper[fname] = dto

        if debug:
            logger.debug(f"Transfer maps => {timekeeper}")
        return tuple(timekeeper.values())
