import os
import sys

sys.path.append("./")
sys.path.append("../evalit/")
sys.path.append("./evalit/")

import evalit

print(evalit.__version__)

from datetime import datetime
from typing import List, Tuple

# read a yaml file
import matplotlib.pyplot as plt
import numpy as np
import yaml
from loguru import logger

from evalit.nifi import NifiAutomation
from evalit.rclone import RcloneAutomation
from evalit.structures import TransferDTO


def dtotimes_to_times(timesdto: Tuple[TransferDTO]) -> List[List[float]]:
    times = map(
        lambda d: [
            (d.start_time - datetime(1970, 1, 1)).total_seconds(),
            (d.end_time - datetime(1970, 1, 1)).total_seconds(),
        ],
        timesdto,
    )
    return list(times)


def generate_grapgs(title: str, timesdto: Tuple[TransferDTO]):
    times = filter(
        lambda t: t.start_time is not None and t.end_time is not None, timesdto
    )
    times = tuple(times)
    times = dtotimes_to_times(times)

    for i in range(len(times)):
        plt.barh(i + 1, times[i][1] - times[i][0], left=times[i][0])

    plt.savefig(title + ".png")


def caclulate_throughput(file_sizes: List[int], timesdto: Tuple[TransferDTO]) -> float:
    timesdto = filter(
        lambda t: t.start_time is not None and t.end_time is not None, timesdto
    )
    times = dtotimes_to_times(timesdto)
    total_valume = 0.0
    for i in range(len(file_sizes)):
        total_valume += file_sizes[i]
    val = 0
    try:
        val = total_valume * 8 / (np.max(times) - np.min(times))
    except ZeroDivisionError:
        logger.error("Error while computing throughput!")
        val = 0
    return val


# TODO: Fetch theses values from the S3 python client
file_sizes = [1]  # GB
file_list = ["testfile"]  # File list in source bucket

# nifi_installation = "/sproj/MFT/nifi-1.15.3"
nifi_installation = os.getenv(
    "NIFI_INSTALLATION", "/home/nishan/software/nifi/nifi-1.15.3"
)
mft_installation = os.getenv("MFT_INSTALLATION", "/proj/MFT/build")

dt_config = os.getenv("CFG_YAML", "tests/config.yaml")

###### Nifi ###########
automation1 = NifiAutomation(
    config=dt_config,
    nifi_url="https://localhost:8443/nifi-api",
    nifi_dir=nifi_installation,
    files=file_list,
)

automation2 = RcloneAutomation(
    dt_config,
    file_list,
    debug=True,
    buffer_size=512,
    multi_thread_streams=10,
    multi_thread_cutoff=50,
    ntransfers=8,
    s3_max_upload_parts=10,
    s3_upload_concurrency=10,
)


# automations = [automation2, automation1]
automations = [automation2]

for automation in automations:
    results: Tuple[TransferDTO] = automation.run_automation()
    logger.info("[{automation.__classname}] Results :: {results}")

    generate_grapgs(automation.__classname__, results)
    throughput = caclulate_throughput(file_sizes, results)
    logger.info(f"[{automation.__classname__}] Throughput = {throughput}")
