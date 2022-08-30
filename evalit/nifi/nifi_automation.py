import os
import random
import string
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple, Union

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from loguru import logger

from .._base import AbstractAutomation
from ..structures import TYPE_PATH, TransferDTO


class NifiAutomation(AbstractAutomation):
    """
    Automation component for Nifi.
    This acts as an interface to `nifi` server.
    (See installation/setup guide for nifi)

    Args:

        ```config```: ```Union[str, pathlib.Path, Dict[str, str]]```
                Configuration for the transfer repo

        ```nifi_url```: ```str```
            URL to communicate with nifi server
            (Usuaally: "https://localhost:8443/nifi-api")

        ```nifi_dir```: ```Union[str, pathlib.Path]```
                Path to the nifi installation directory
                (Normally, we get this from the environment
                variable `NIFI_INSTALLATION`)


        ```files```: ```List[str]```
            List of filenames to be transferred.
            (Currently it's not used in RcloneAutomation)

        ```xml_conf```: ```Optional[str]```
            Path to the nifi s3 xml configuration.
            Defaults to `nifi/nifi-s3.xml` package file in the transfer repo.
            This provides more configurability/control for transfer using nifi.

        ```debug```: ```bool```
            Debug mode flag. Defaults to False.

        ```params```: ```Any```
            keyword args provided for nifi transfer, misc.
            (Currently not used.)

    Note:
        1) Since, nifi transfers happen in async mode -- jobs are submitted
        to initiate transfer -- we need to parse nifi log every N seconds
        (see `nifi_log_poll_time` kwarg to
        `NifiAutomation.run_automation(...)`)
        to figure out the status of the transfer. This poll-based log parsing
        is also sued in `MftAutomation`.
    """

    _RESOURCES_CFG = {
        "template": "nifi-s3.xml",
        "log": "logs/nifi-app.log",
    }

    # these are used for parsing nifi log
    _LOG_START_PHRASE = "Starting the data transfer"
    _LOG_COMPLETE_PHRASE = "Completed the transfer"

    def __init__(
        self,
        config: Union[Dict[str, str], TYPE_PATH],
        nifi_url: str,
        nifi_dir: TYPE_PATH,
        files: Optional[Sequence[TYPE_PATH]] = None,
        xml_conf: Optional[str] = None,
        debug: bool = False,
        **params,
    ) -> None:
        super().__init__(config=config, files=files, debug=debug)
        self.nifi_url = nifi_url

        assert os.path.exists(nifi_dir), f"{nifi_dir} path doesn't exist!"
        self.nifi_dir = nifi_dir

        if xml_conf is not None:
            assert os.path.exists(xml_conf), f"{xml_conf} path doesn't exist!"
        self.xml_conf = xml_conf

    def run_automation(self, **kwargs) -> Tuple[TransferDTO]:
        """
        Runs nifi automation

        Args:
            ```kwargs```: ```Any```
                extra params to be used for the transfer.
                - `nifi_log_poll_time`
                    - every N seconds to parse nifi log file to figure out
                    if transfer is complete.

        Returns:
            Tuple of individual file data transfer `Tuple[TransferDTO]`,
            where each element object stores
            - filename
            - start_time
            - end_time
            - transferer ("rclone")
        """
        start_automation = time.time()
        logger.info(f"Running automation for {self.__classname__}")

        random_string = lambda string_length: "".join(
            random.choice(string.ascii_lowercase) for i in range(string_length)
        )

        # nifi_url = "https://localhost:8443/nifi-api"
        nifi_url = self.nifi_url
        template_file = self.xml_conf or (
            Path(__file__)
            .parent.joinpath(self._RESOURCES_CFG["template"])
            .absolute()
            .as_posix()
        )
        logger.debug(f"Using template: {template_file}")

        log_file_location = os.path.join(self.nifi_dir,
                                         self._RESOURCES_CFG["log"])
        logger.debug(f"log_file_location = {log_file_location}")

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

        session_uuid = random_string(10)
        logger.info(f"Session UUID: {session_uuid}")

        r = requests.get(
            nifi_url + "/flow/process-groups/root?uiOnly=true", verify=False
        )
        process_group_id = r.json()["processGroupFlow"]["id"]

        # Stopping the process group before resetting the template

        logger.info(f"Stopping process group = {process_group_id}")
        update_process_group_json = {
            "id": process_group_id,
            "state": "STOPPED",
            "disconnectedNodeAcknowledged": "false",
        }
        r = requests.put(
            nifi_url + "/flow/process-groups/" + process_group_id,
            json=update_process_group_json,
            verify=False,
        )
        logger.debug(f"Status = {r.status_code}")

        # Deleting the existing flow

        r = requests.get(
            nifi_url + "/flow/process-groups/" + process_group_id, verify=False
        )

        process_groups_json = r.json()
        old_connections = process_groups_json["processGroupFlow"]
        ["flow"]["connections"]

        for connection in old_connections:
            if self.debug:
                print("Deleting connection " + connection["id"])
            r = requests.delete(
                nifi_url + "/connections/" + connection["id"],
                params={"version": str(connection["revision"]["version"])},
                verify=False,
            )
            if self.debug:
                print("Status", r.status_code)

        old_processors = process_groups_json["processGroupFlow"]
        ["flow"]["processors"]
        for processor in old_processors:
            if self.debug:
                print("Deleting processor ", processor["id"])
            r = requests.delete(
                nifi_url + "/processors/" + processor["id"],
                params={"version": str(processor["revision"]["version"])},
                verify=False,
            )
            if self.debug:
                print("Status", r.status_code)

        # Deletes all template files

        r = requests.get(nifi_url + "/flow/templates", verify=False)
        templates = r.json()["templates"]

        for template in templates:
            if self.debug:
                print(
                    "Deleting template ", template["id"],
                    template["template"]["name"]
                )
            requests.delete(nifi_url + "/templates/" +
                            template["id"], verify=False)

            # Upload the template file

        template_upload_json = {
            "template": (
                template_file,
                open(template_file, "rb"),
                "multipart/form-data",
            )
        }
        r = requests.post(
            nifi_url + "/process-groups/" + process_group_id +
            "/templates/upload",
            files=template_upload_json,
            verify=False,
        )

        print("Template successfully uploaded")
        id_pos = r.text.find("<id>")
        id_end_pos = r.text.find("</id>")
        template_id = r.text[id_pos + 4: id_end_pos]
        print("Template id: " + template_id)

        template_load_json = {
            "templateId": template_id,
            "originX": 611.2981057221397,
            "originY": 85.35885905334999,
            "disconnectedNodeAcknowledged": "false",
        }
        r = requests.post(
            nifi_url + "/process-groups/" + process_group_id +
            "/template-instance",
            json=template_load_json,
            verify=False,
        )

        template_json = r.json()
        processor_name_map = {}
        for processor in template_json["flow"]["processors"]:
            processor_name_map[processor["component"]
                               ["name"]] = processor

        # Updating the credentials

        list_s3_processor = processor_name_map["ListS3"]
        list_s3_update_json = {
            "component": {
                "id": list_s3_processor["id"],
                "name": "ListS3",
                "config": {
                    "schedulingPeriod": "0 sec",
                    "executionNode": "PRIMARY",
                    "penaltyDuration": "30 sec",
                    "yieldDuration": "1 sec",
                    "bulletinLevel": "WARN",
                    "schedulingStrategy": "TIMER_DRIVEN",
                    "comments": "",
                    "autoTerminatedRelationships": [],
                    "properties": {
                        "Bucket": source_s3_bucket,
                        "Access Key": source_token,
                        "Secret Key": source_secret,
                        "Endpoint Override URL": source_s3_endpoint,
                        "Region": source_s3_region,
                    },
                },
                "state": "STOPPED",
            },
            "revision": {"version": list_s3_processor["revision"]["version"]},
            "disconnectedNodeAcknowledged": "false",
        }

        if self.debug:
            print("Updating ListS3 processor")
        r = requests.put(
            nifi_url + "/processors/" + list_s3_processor["id"],
            json=list_s3_update_json,
            verify=False,
        )
        if self.debug:
            print("Status", r.status_code)

        fetch_s3_processor = processor_name_map["FetchS3Object"]
        fetch_s3_update_json = {
            "component": {
                "id": fetch_s3_processor["id"],
                "name": "FetchS3Object",
                "config": {
                    "concurrentlySchedulableTaskCount": "10",
                    "schedulingPeriod": "0 sec",
                    "executionNode": "ALL",
                    "penaltyDuration": "30 sec",
                    "yieldDuration": "1 sec",
                    "bulletinLevel": "WARN",
                    "schedulingStrategy": "TIMER_DRIVEN",
                    "comments": "",
                    "runDurationMillis": 0,
                    "autoTerminatedRelationships": ["failure"],
                    "properties": {
                        "Bucket": source_s3_bucket,
                        "Access Key": source_token,
                        "Secret Key": source_secret,
                        "Endpoint Override URL": source_s3_endpoint,
                        "Region": source_s3_region,
                    },
                },
                "state": "STOPPED",
            },
            "revision": {"version": fetch_s3_processor["revision"]["version"]},
            "disconnectedNodeAcknowledged": "false",
        }

        print("Updating FetchS3Object processor")
        r = requests.put(
            nifi_url + "/processors/" + fetch_s3_processor["id"],
            json=fetch_s3_update_json,
            verify=False,
        )
        print("Status", r.status_code)

        put_s3_processor = processor_name_map["PutS3Object"]
        put_s3_update_json = {
            "component": {
                "id": put_s3_processor["id"],
                "name": "PutS3Object",
                "config": {
                    "concurrentlySchedulableTaskCount": "10",
                    "schedulingPeriod": "0 sec",
                    "executionNode": "ALL",
                    "penaltyDuration": "30 sec",
                    "yieldDuration": "1 sec",
                    "bulletinLevel": "WARN",
                    "schedulingStrategy": "TIMER_DRIVEN",
                    "comments": "",
                    "runDurationMillis": 0,
                    "autoTerminatedRelationships": ["failure"],
                    "properties": {
                        "Bucket": dest_s3_bucket,
                        "Access Key": dest_token,
                        "Secret Key": dest_secret,
                        "Endpoint Override URL": dest_s3_endpoint,
                        "Region": dest_s3_region,
                    },
                },
                "state": "STOPPED",
            },
            "revision": {"version": put_s3_processor["revision"]["version"]},
            "disconnectedNodeAcknowledged": "false",
        }

        print("Updating PutS3Object processor")
        r = requests.put(
            nifi_url + "/processors/" + put_s3_processor["id"],
            json=put_s3_update_json,
            verify=False,
        )
        print("Status", r.status_code)

        print("Updating Start trnsfer log processor")
        start_log_processor = processor_name_map["Started transfer"]
        start_log_update_json = {
            "component": {
                "id": start_log_processor["id"],
                "name": "Started transfer",
                "config": {
                    "concurrentlySchedulableTaskCount": "1",
                    "schedulingPeriod": "0 sec",
                    "executionNode": "ALL",
                    "penaltyDuration": "30 sec",
                    "yieldDuration": "1 sec",
                    "bulletinLevel": "WARN",
                    "schedulingStrategy": "TIMER_DRIVEN",
                    "comments": "",
                    "runDurationMillis": 0,
                    "autoTerminatedRelationships": ["success"],
                    "properties": {
                        "log-message": f"{self._LOG_START_PHRASE} "
                        + session_uuid
                        + " ${filename}"
                    },
                },
                "state": "STOPPED",
            },
            "revision": {"version": start_log_processor["revision"]
                         ["version"]},
            "disconnectedNodeAcknowledged": "false",
        }
        r = requests.put(
            nifi_url + "/processors/" + start_log_processor["id"],
            json=start_log_update_json,
            verify=False,
        )
        print("Status", r.status_code)

        print("Updating Start trnsfer log processor")
        complete_log_processor = processor_name_map["Completed transfer"]
        complete_log_update_json = {
            "component": {
                "id": complete_log_processor["id"],
                "name": "Completed transfer",
                "config": {
                    "concurrentlySchedulableTaskCount": "1",
                    "schedulingPeriod": "0 sec",
                    "executionNode": "ALL",
                    "penaltyDuration": "30 sec",
                    "yieldDuration": "1 sec",
                    "bulletinLevel": "WARN",
                    "schedulingStrategy": "TIMER_DRIVEN",
                    "comments": "",
                    "runDurationMillis": 0,
                    "autoTerminatedRelationships": ["success"],
                    "properties": {
                        "log-message": f"{self._LOG_COMPLETE_PHRASE} "
                        + session_uuid
                        + " ${filename}"
                    },
                },
                "state": "STOPPED",
            },
            "revision": {"version": complete_log_processor["revision"]
                         ["version"]},
            "disconnectedNodeAcknowledged": "false",
        }
        r = requests.put(
            nifi_url + "/processors/" + complete_log_processor["id"],
            json=complete_log_update_json,
            verify=False,
        )
        print("Updating Complete trnsfer log processor")

        # Starting the process group
        #
        print("Starting process group")
        update_process_group_json = {
            "id": process_group_id,
            "state": "RUNNING",
            "disconnectedNodeAcknowledged": "false",
        }
        r = requests.put(
            nifi_url + "/flow/process-groups/" + process_group_id,
            json=update_process_group_json,
            verify=False,
        )
        print("Status", r.status_code)

        vals = self.parse_log(
            log=log_file_location,
            nfiles=len(self.files),
            session_uuid=session_uuid,
            poll_wait_time=kwargs.get("nifi_log_poll_time", 5) or 5,
        )
        logger.debug(
            f"Delta time for {self.__classname__} = "
            f"{time.time() - start_automation}"
        )
        return vals

    def parse_log(
        self, log: str, nfiles: int, session_uuid: str, poll_wait_time: int = 5
    ) -> Tuple[TransferDTO]:
        """
        Parse nifi log based on polling mechanism.
        This uses some string-matching line-by-line of the log file.

        The string matching happens through patterns:
            - NifiAutomation._LOG_START_PHRASE
            - NifiAutomation._LOG_COMPLETE_PHRASE

        Args:

            ```log```: ```str```
                Path to the nifi log file

            ```nfiles```: ```int```
                Total number of files that are being transferred.
                (This is used to compute "when to stop" the poll)

            ```session_uuid```: ```str```
                Nifi transfer session id

            ```poll_wait_time```: ```int```
                Every `poll_wait_time` seconds, nifi log is parsed,
                and decide if transfer is complete

        Returns:
            Tuple of individual file data transfer `Tuple[TransferDTO]`,
            where each element object stores
            - filename
            - start_time
            - end_time
            - transferer ("nifi")
        """
        timekeeper = {}
        end_counter = 0

        # We need to poll the logging
        # till we all the `nfiles` are detected to be "transferred".
        while end_counter < nfiles:
            logger.debug(
                f"[{self.__classname__}] log parser polling..."
                f"{end_counter}/{nfiles} files transferred!"
            )
            time.sleep(poll_wait_time)
            # read file line by line
            with open(log, "r") as f:
                # reset counter
                end_counter = 0
                for line in f:
                    if line.find(session_uuid) != -1:
                        utc_time = datetime.strptime(
                            line.split(",")[0], "%Y-%m-%d %H:%M:%S"
                        )
                        fname = line.split(session_uuid)[1].strip()
                        is_start = (
                            line.split(session_uuid)[0].
                            find(self._LOG_START_PHRASE)
                            > -1
                        )
                        is_complete = (
                            line.split(session_uuid)[0].
                            find(self._LOG_COMPLETE_PHRASE)
                            > -1
                        )

                        dto = timekeeper.get(
                            fname, TransferDTO(fname=fname, transferer="nifi")
                        )
                        if is_start:
                            dto.start_time = utc_time
                        if is_complete:
                            dto.end_time = utc_time
                            end_counter += 1
                        timekeeper[fname] = dto
        return tuple(timekeeper.values())
