import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

import urllib3
import yaml
from loguru import logger


class RcloneAutomation:
    def __init__(self, config_yaml, file_list) -> None:
        with open(config_yaml) as f:
            # self.config = yaml.load(f)
            self.config = yaml.safe_load(f)
            self.sourceName = "s3source"
            self.destName = "s3dest"
            self.buffer_size = 50
            self.multi_thread_streams = 10
            self.multi_thread_cutoff = 50
            self.ntransfers = 8
            self.s3_max_upload_parts = 10
            self.s3_upload_concurrency = 10
            self.file_list = file_list

    def run_automation(self):

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        source_token = self.config["source_token"]
        source_secret = self.config["source_secret"]
        source_s3_endpoint = self.config["source_s3_endpoint"]
        source_s3_bucket = self.config["source_s3_bucket"]
        source_s3_region = self.config["source_s3_region"]

        dest_token = self.config["dest_token"]
        dest_secret = self.config["dest_secret"]
        dest_s3_endpoint = self.config["dest_s3_endpoint"]
        dest_s3_bucket = self.config["dest_s3_bucket"]
        dest_s3_region = self.config["dest_s3_region"]

        total_files = len(self.file_list)

        rclone_log_file = (
            Path(__file__).parent.joinpath("rclone.log").absolute().as_posix()
        )
        logger.debug(f"rclone log file :: {rclone_log_file}")

        rclone_config_file = (
            Path(__file__).parent.joinpath("rclone.conf").absolute().as_posix()
        )
        logger.debug(f"rclone conf file :: {rclone_config_file}")

        if os.path.exists(rclone_log_file):
            os.remove(rclone_log_file)
            print("Deleting ", rclone_log_file)

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

        with open(rclone_config_file, "w") as f:
            f.writelines(lines)

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
            f"--config={rclone_config_file}",
            f"--log-file={rclone_log_file}",
            "--log-level=DEBUG",
            "-I",
        ]

        logger.debug(" ".join(cmd))
        start = time.time()
        process = subprocess.Popen(
            cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        logger.debug(f"Execution took {time.time()-start} seconds.")
        # print(stdout, stderr)

        start_time_map = {}
        end_time_map = {}

        with open(rclone_log_file, "r") as f:
            for line in f:
                if (
                    line.find("multipart upload starting chunk 1 size") != -1
                    or line.find("Transferring unconditionally") != -1
                ):
                    print(line.split(":")[3].strip(), line.split("DEBUG")[0].strip())
                    utc_time = datetime.strptime(
                        line.split("DEBUG")[0].strip(), "%Y/%m/%d %H:%M:%S"
                    )
                    start_time_map[line.split(":")[3].strip()] = utc_time

                if line.find("Copied") != -1:
                    print(line.split(":")[3].strip(), line.split("INFO")[0].strip())
                    utc_time = datetime.strptime(
                        line.split("INFO")[0].strip(), "%Y/%m/%d %H:%M:%S"
                    )
                    end_time_map[line.split(":")[3].strip()] = utc_time

        final_time_array = []

        for key, start_time in start_time_map.items():
            end_time = end_time_map[key]
            print(key, (end_time - start_time).total_seconds())
            final_time_array.append(
                [
                    (start_time - datetime(1970, 1, 1)).total_seconds(),
                    (end_time - datetime(1970, 1, 1)).total_seconds(),
                ]
            )

        return final_time_array
