import time
from datetime import datetime
from typing import List, Tuple

import numpy as np
from loguru import logger

try:
    import matplotlib.pyplot as plt

    MATPLOTLIB = True
except ModuleNotFoundError:
    MATPLOTLIB = False
    logger.warning("matplotlib not found!")


from ._base import AbstractController
from .structures import TransferDTO


class StandardAutomationController(AbstractController):
    """
    This is a standard implementation of the Automation Controller.
    The run method:
        - takes in filemap through kwargs
        - runs all the available automation (Type[AbstractAutomation])
        - computes throughput for each
        - generate bar graph
    """

    def run(self, **kwargs) -> None:
        """
        This is the main entrypoint to the controller.
        It abstracts all the transfers and computation.
        """
        logger.info("Controller has started...")
        controller_start = time.time()

        filemap = kwargs.get("filemap", {})
        logger.debug(f"Total files in filemap => {len(filemap)}")
        if not filemap:
            logger.warning("No files found! Aborting the runs!")
            return

        file_sizes = tuple(map(lambda x: x["size"], filemap.values()))
        logger.debug(f"Total size of all file blobs => {(sum(file_sizes))}")

        for automation in self.automations:
            results: Tuple[TransferDTO] = automation.run_automation(**kwargs)
            results = tuple(
                filter(
                    lambda r: r.start_time is not None and r.end_time is not None,
                    results,
                )
            )
            if self.debug:
                logger.debug(f"[{automation.__classname__}] Results :: {results}")

            # filter results based on filemap
            results_filemapped = tuple(filter(lambda r: r.fname in filemap, results))

            # in some automation (like MFT), fname are temp ids returned by
            # the transfer. So, in that case, no file matches.
            results_filemapped = (
                results if not results_filemapped else results_filemapped
            )

            file_sizes_filemapped = tuple(
                map(lambda r: filemap[r.fname]["size"], results_filemapped)
            )
            file_sizes_filemapped = (
                file_sizes if not file_sizes_filemapped else file_sizes_filemapped
            )

            throughput = self.caclulate_throughput(
                file_sizes_filemapped, results_filemapped
            )
            logger.info(f"[{automation.__classname__}] Throughput = {throughput} Gbps.")

            self.generate_grapgs(automation.__classname__, results)

        logger.info(
            f"Controller took total {time.time()-controller_start} seconds to run!"
        )

    def generate_grapgs(self, title: str, timesdto: Tuple[TransferDTO]):
        if not MATPLOTLIB:
            logger.warning("Matplotlib not found. Can't generate figure! Halting!")
            return

        times = self.dtotimes_to_times(timesdto)
        for i in range(len(times)):
            plt.barh(i + 1, times[i][1] - times[i][0], left=times[i][0])
        plt.savefig(title + ".png")

    def caclulate_throughput(
        self, file_sizes: List[int], timesdto: Tuple[TransferDTO]
    ) -> float:
        """
        Calculate transfer throughput in terms of Gbps
        """
        times = self.dtotimes_to_times(timesdto)
        total_volume = sum(file_sizes)
        val = 0
        try:
            val = total_volume * 8 / (float(np.max(times)) - float(np.min(times)))
        except ZeroDivisionError:
            logger.error("Error while computing throughput!")
            val = 0
        return round(val, 3)

    @staticmethod
    def dtotimes_to_times(timesdto: Tuple[TransferDTO]) -> List[List[float]]:
        times = map(
            lambda d: [
                (d.start_time - datetime(1970, 1, 1)).total_seconds(),
                (d.end_time - datetime(1970, 1, 1)).total_seconds(),
            ],
            timesdto,
        )
        return list(times)
