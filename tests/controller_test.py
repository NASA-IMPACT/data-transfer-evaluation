import sys

sys.path.append("./")
sys.path.append("../evalit/")
sys.path.append("./evalit/")

import evalit

print(evalit.__version__)

# read a yaml file
import matplotlib.pyplot as plt
import numpy as np
import yaml

# from mft.mft_automation import MFTAutomation
# from nifi.nifi_automation import NifiAutomation
print(dir(evalit))
from evalit.rclone import RcloneAutomation


def generate_grapgs(title, times):
    times = times - np.min(times)

    for i in range(len(times)):
        plt.barh(i + 1, times[i][1] - times[i][0], left=times[i][0])

    plt.savefig(title + ".png")


def caclulate_throughput(file_sizes, times):
    total_valume = 0.0
    for i in range(len(file_sizes)):
        total_valume += file_sizes[i]
    throughput = total_valume * 8 / (np.max(times) - np.min(times))
    print("Throughput: " + str(throughput))


# TODO: Fetch theses values from the S3 python client
file_sizes = [1]  # GB
file_list = ["testfile"]  # File list in source bucket

# nifi_installation = "/sproj/MFT/nifi-1.15.3"
nifi_installation = "/home/nishan/software/nifi/nifi-1.15.3"
mft_installation = "/proj/MFT/build"

config_file = "evalit/config.yaml"

###### Nifi ###########
# automation = NifiAutomation(config_file, file_list, nifi_installation)
# nifi_result = automation.run_automation()
# print("Nifif automation results")
# print(nifi_result)

# generate_grapgs("nifi", nifi_result)
# caclulate_throughput(file_sizes, nifi_result)

###### Rclone ###########
# automation = RcloneAutomation(
#     config_file,
#     file_list,
#     buffer_size=512,
#     multi_thread_streams=10,
#     multi_thread_cutoff=50,
#     ntransfers=8,
#     s3_max_upload_parts=10,
#     s3_upload_concurrency=10,
# )
automation = RcloneAutomation.from_yaml(
    config_file,
    file_list,
    buffer_size=512,
    multi_thread_streams=10,
    multi_thread_cutoff=50,
    ntransfers=8,
    s3_max_upload_parts=10,
    s3_upload_concurrency=10,
)
rclone_result = automation.run_automation()
print("Rclone automation results", rclone_result)

generate_grapgs("rclone", rclone_result)
caclulate_throughput(file_sizes, rclone_result)

###### Airavata MFT ###########
# automation = MFTAutomation(config_file, file_list, mft_installation)
# mft_result = automation.run_automation()
# print("Rclone automation results", mft_result)

# generate_grapgs("mft", mft_result)
# caclulate_throughput(file_sizes, mft_result)
