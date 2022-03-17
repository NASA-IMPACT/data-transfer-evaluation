from time import sleep
import urllib3
from datetime import datetime
import yaml
import subprocess
import os
from joblib import Parallel, delayed
import multiprocessing

class MFTAutomation:

    def __init__(self, config_yaml, file_list, mft_dir) -> None:
        with open(config_yaml) as f:
            self.config = yaml.load(f)
            self.file_list = file_list
            self.mft_dir = mft_dir


    def submit_transfer(self, file_name, source_storage_id, dest_storage_id):
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
            "S3"
        ]
        process = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        stdout = stdout.decode("utf-8")
        transferId = stdout.split("Submitted Transfer ")[1].strip()
        print("Fetched Trasnfer id ", transferId)
        return transferId

    def run_automation(self):
        source_storage_id = "4760ae31-923c-47ed-9a01-e3c6ad303a92"
        dest_storage_id = "d602acab-595a-44ac-a9b3-60c5ceca80e7"

        num_cores = multiprocessing.cpu_count()
        transfer_ids = Parallel(n_jobs=num_cores)(delayed(self.submit_transfer)(file_name, source_storage_id, dest_storage_id) for file_name in self.file_list)


        start_time_map = {}
        end_time_map = {}

        while(1):
            sleep(5)

            for transfer_id in transfer_ids:
                cmd = [
                    "java",
                    "-jar",
                    self.mft_dir + "/mft-client.jar",
                    "transfer",
                    "state",
                    "-a",
                    transfer_id
                ]
                process = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = process.communicate()
                stdout = stdout.decode("utf-8")
                print(stdout)
                parts = stdout.split("\n")
                for part in parts:
                    if part.find("STARTING") != -1:
                        start_time = part.split("|")[1].strip()
                        start_time_map[transfer_id] = int(start_time)/1000
                        print("Start time: ", start_time)

                    if part.find("COMPLETED") != -1:
                        end_time = part.split("|")[1].strip()
                        end_time_map[transfer_id] = int(end_time)/1000
                        print("End time: ", end_time)

            if len(start_time_map) == len(transfer_ids) and len(end_time_map) == len(transfer_ids):
                break

        final_time_array = []


        for key, start_time in start_time_map.items():
            end_time = end_time_map[key]
            print(key, end_time - start_time)
            final_time_array.append([start_time, end_time ])

        return final_time_array