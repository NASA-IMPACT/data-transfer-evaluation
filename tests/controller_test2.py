import multiprocessing
import os
import sys

import boto3

sys.path.append("./")
sys.path.append("../evalit/")
sys.path.append("./evalit/")

import evalit

print(f"automation pkg version: {evalit.__version__}")

from loguru import logger

from evalit.api import MFTAutomation, NifiAutomation, RcloneAutomation
from evalit.controller import StandardAutomationController

nifi_installation = os.getenv("NIFI_INSTALLATION")
mft_installation = os.getenv("MFT_INSTALLATION")

dt_config = os.getenv("CFG_YAML", "tests/config.yaml")
dt_config = RcloneAutomation.load_yaml(dt_config)

filemap = StandardAutomationController.get_source_file_map(dt_config)
filenames = tuple(filemap.keys())
logger.debug(filemap)

# build controller with available automation components
controller = (
    StandardAutomationController(debug=True)
    .add_automation(
        RcloneAutomation(
            dt_config,
            filenames,
            debug=False,
            buffer_size=512,
            multi_thread_streams=10,
            multi_thread_cutoff=50,
            ntransfers=8,
            s3_max_upload_parts=10,
            s3_upload_concurrency=10,
        )
    )
    .add_automation(
        NifiAutomation(
            config=dt_config,
            nifi_url="https://localhost:8443/nifi-api",
            nifi_dir=nifi_installation,
            files=filenames,
            xml_conf=os.getenv("NIFI_XML_CONF"),
            debug=False,
        )
    )
    .add_automation(
        MFTAutomation(
            config=dt_config,
            mft_dir=mft_installation,
            files=filenames,
            njobs=multiprocessing.cpu_count(),
        )
    )
)
results = controller.run(
    filemap=filemap,
    nifi_log_poll_time=5,
    mft_log_poll_time=5,
    mft_log_parser_njobs=multiprocessing.cpu_count(),
)
logger.info(results)
