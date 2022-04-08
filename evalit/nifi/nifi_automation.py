import os
import random
import string
import tempfile
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
    _RESOURCES_CFG = {
        "template": "nifi-s3.xml",
        "log": "logs/nifi-app.log",
    }

    def __init__(
        self,
        config: Union[Dict[str, str], TYPE_PATH],
        nifi_url: str,
        nifi_dir: TYPE_PATH,
        files: Optional[Sequence[TYPE_PATH]] = None,
        debug: bool = False,
        **params,
    ) -> None:
        super().__init__(config=config, files=files, debug=debug)
        self.nifi_url = nifi_url

        assert os.path.exists(nifi_dir), f"{nifi_dir} path doesn't exist!"
        self.nifi_dir = nifi_dir

    def run_automation(self):
        start_automation = time.time()
        logger.info(f"Running automation for {self.__classname__}")

        random_string = lambda string_length: "".join(
            random.choice(string.ascii_lowercase) for i in range(string_length)
        )

        # nifi_url = "https://localhost:8443/nifi-api"
        nifi_url = self.nifi_url
        template_file = (
            Path(__file__)
            .parent.joinpath(self._RESOURCES_CFG["template"])
            .absolute()
            .as_posix()
        )

        log_file_location = os.path.join(self.nifi_dir, self._RESOURCES_CFG["log"])
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

        total_files = len(self.files)

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
        old_connections = process_groups_json["processGroupFlow"]["flow"]["connections"]

        for connection in old_connections:
            print("Deleting connection " + connection["id"])
            r = requests.delete(
                nifi_url + "/connections/" + connection["id"],
                params={"version": str(connection["revision"]["version"])},
                verify=False,
            )
            print("Status", r.status_code)

        old_processors = process_groups_json["processGroupFlow"]["flow"]["processors"]
        for processor in old_processors:
            print("Deleting processor ", processor["id"])
            r = requests.delete(
                nifi_url + "/processors/" + processor["id"],
                params={"version": str(processor["revision"]["version"])},
                verify=False,
            )
            print("Status", r.status_code)

        # Deletes all template files

        r = requests.get(nifi_url + "/flow/templates", verify=False)
        templates = r.json()["templates"]

        for template in templates:
            print("Deleting template ", template["id"], template["template"]["name"])
            requests.delete(nifi_url + "/templates/" + template["id"], verify=False)

            # Upload the template file

        template_upload_json = {
            "template": (
                template_file,
                open(template_file, "rb"),
                "multipart/form-data",
            )
        }
        r = requests.post(
            nifi_url + "/process-groups/" + process_group_id + "/templates/upload",
            files=template_upload_json,
            verify=False,
        )

        print("Template successfully uploaded")
        id_pos = r.text.find("<id>")
        id_end_pos = r.text.find("</id>")
        template_id = r.text[id_pos + 4 : id_end_pos]
        print("Template id: " + template_id)

        template_load_json = {
            "templateId": template_id,
            "originX": 611.2981057221397,
            "originY": 85.35885905334999,
            "disconnectedNodeAcknowledged": "false",
        }
        r = requests.post(
            nifi_url + "/process-groups/" + process_group_id + "/template-instance",
            json=template_load_json,
            verify=False,
        )

        template_json = r.json()
        processor_name_map = {}
        for processor in template_json["flow"]["processors"]:
            processor_name_map[processor["component"]["name"]] = processor

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

        print("Updating ListS3 processor")
        r = requests.put(
            nifi_url + "/processors/" + list_s3_processor["id"],
            json=list_s3_update_json,
            verify=False,
        )
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
                        "log-message": "Starting the data transfer "
                        + session_uuid
                        + " ${filename}"
                    },
                },
                "state": "STOPPED",
            },
            "revision": {"version": start_log_processor["revision"]["version"]},
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
                        "log-message": "Completed the transfer "
                        + session_uuid
                        + " ${filename}"
                    },
                },
                "state": "STOPPED",
            },
            "revision": {"version": complete_log_processor["revision"]["version"]},
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
            poll_wait_time=5,
        )
        logger.debug(
            f"Delta time for {self.__classname__} = {time.time() - start_automation}"
        )
        return vals

    def parse_log(
        self, log: str, nfiles: int, session_uuid: str, poll_wait_time: int = 5
    ) -> Tuple[TransferDTO]:
        timekeeper = {}
        end_counter = 0

        # TODO: Optimize this!
        while (len(timekeeper) < nfiles) and (end_counter < nfiles):
            time.sleep(poll_wait_time)
            # read file line by line
            with open(log, "r") as f:
                for line in f:
                    if line.find(session_uuid) != -1:
                        utc_time = datetime.strptime(
                            line.split(",")[0], "%Y-%m-%d %H:%M:%S"
                        )
                        fname = line.split(session_uuid)[1].strip()
                        is_start = line.split(session_uuid)[0].find("Starting") > -1

                        dto = timekeeper.get(
                            fname, TransferDTO(fname=fname, transferer="nifi")
                        )
                        if is_start:
                            dto.start_time = utc_time
                        else:
                            dto.end_time = utc_time
                            end_counter += 1
                        timekeeper[fname] = dto
        return tuple(timekeeper.values())
