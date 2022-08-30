import multiprocessing
import os

from loguru import logger

import evalit
from evalit.api import BaseAPIs as EvalBases
from evalit.api import (
    MFTAutomation,
    NifiAutomation,
    RcloneAutomation,
    StandardAutomationController,
)

print(f"automation pkg version: {evalit.__version__}")

ncpus = multiprocessing.cpu_count()
logger.info(f"N cpus = {ncpus}")

nifi_installation = os.getenv("NIFI_INSTALLATION")
mft_installation = os.getenv("MFT_INSTALLATION")

dt_config = os.getenv("CFG_YAML", "tests/config.yaml")
dt_config = EvalBases.AbstractAutomation.load_yaml(dt_config)
logger.debug(
    f"Redacted config::"
    f"{EvalBases.AbstractAutomation._get_redacted_cfg(dt_config)}"
)

filemap = StandardAutomationController.get_source_file_map(dt_config)
filenames = tuple(filemap.keys())
logger.debug(f"filemap:: {filemap}")


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
            ntransfers=ncpus,
            s3_max_upload_parts=10,
            s3_upload_concurrency=16,
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
            njobs=ncpus,
        )
    )
)
results = controller.run(
    filemap=filemap,
    nifi_log_poll_time=5,
    mft_log_poll_time=5,
    mft_log_parser_njobs=ncpus,
)
logger.info(results)
