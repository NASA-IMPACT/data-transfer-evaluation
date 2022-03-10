from nifi.nifi_automation import NifiAutomation
from mft.mft_automation import MFTAutomation
from odata.odata_automation import OdataAutomation
from rclone.rclone_automation import RcloneAutomation

import numpy as np
import matplotlib.pyplot as plt

def generate_graphs(title, times):
    times = times - np.min(times)

    for i in range(len(times)):
        plt.barh(i + 1, times[i][1] - times[i][0], left = times[i][0])

    plt.savefig(title + ".png")

def caclulate_throughput(file_sizes, times):
    total_valume = 0.0
    for i in range(len(file_sizes)):
        total_valume += file_sizes[i]
    throughput = total_valume * 8/ (np.max(times) - np.min(times))
    print("Throughput: " + str(throughput))


file_sizes = [0.2, 0.2, 0.2]

# Nifi

automation = NifiAutomation("configs.yaml")
nifi_result = automation.run_automation()

generate_graphs("nifi", nifi_result)
caclulate_throughput(file_sizes, nifi_result)

#MFT

automation = MFTAutomation("configs.yaml")
mft_result = automation.run_automation()

generate_graphs("mft", nifi_result)
caclulate_throughput(file_sizes, nifi_result)

# Rclone

automation = RcloneAutomation("configs.yaml")
rclone_result = automation.run_automation()

generate_graphs("rclone", nifi_result)
caclulate_throughput(file_sizes, nifi_result)

# Odata

automation = OdataAutomation("configs.yaml")
odata_result = automation.run_automation()

generate_graphs("odata", nifi_result)
caclulate_throughput(file_sizes, nifi_result)