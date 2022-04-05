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


from ._base import AbstractAutomation, AbstractController
from .structures import TransferDTO


class StandardAutomationController(AbstractController):
    def run(self, **kwargs):
        logger.info("Controller has started...")

        filemap = kwargs.get("filemap", {})
        file_vals = tuple(filemap.values())
        file_sizes = tuple(map(lambda x: x[0], file_vals))

        for automation in self.automations:
            results: Tuple[TransferDTO] = automation.run_automation()
            logger.info(f"[{automation.__classname__}] Results :: {results}")

            results = tuple(
                filter(
                    lambda t: t.start_time is not None and t.end_time is not None,
                    results,
                )
            )

            throughput = self.caclulate_throughput(file_sizes, results)
            logger.info(f"[{automation.__classname__}] Throughput = {throughput}")
            self.generate_grapgs(automation.__classname__, results)

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
            val = total_volume * 8 / (np.max(times) - np.min(times))
        except ZeroDivisionError:
            logger.error("Error while computing throughput!")
            val = 0
        return val

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
