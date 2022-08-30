import pathlib
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
from .structures import TYPE_PATH, TransferDTO


class StandardAutomationController(AbstractController):
    """
    This is a standard implementation of the Automation Controller.
    The run method:
        - takes in filemap through kwargs
        - runs all the available automation (Type[AbstractAutomation])
        - computes throughput for each
        - generate bar graph
    """

    def run(self, **kwargs) -> dict:
        """
        This is the main entrypoint to the controller.
        It abstracts all the transfers and computation.
        It goes through all the available `Type[AbstractAutomation]` objects
        (inside `self.automations`) and perform the transfer,
        track files and benchmark.

        Args:
            ```kwargs```: ```Any```
                Extra params that are used by controller for success.
                (See kwargs section below.)

        Kwargs:
            ```filemap```: ```Dict[str, dict]```
                Represents the metadata information for each file.
                - Key is filename
                - Value is another dictionary that has size information.
            ```nifi_log_poll_time```: ```int```
                Time in seconds used by nifi log parser
                (See `nifi.NifiAutomation`)
            ```mft_log_poll_time```: ```int```
                Time in seconds used by mft log parser
                (See `mft.MftAutomation`)
            ```mft_log_parser_njobs```: ```int```
                Number of parallelism for mft automation component.
                Defaults to total number of CPUs detected.

        Returns:
            Overall evaluation result (dict)
        """
        controller_start = time.time()
        logger.info("Controller has started...")

        filemap = kwargs.get("filemap", {})
        logger.debug(f"Total files in filemap => {len(filemap)}")

        file_sizes = tuple(map(lambda x: x["size"], filemap.values()))
        logger.debug(f"Total size of all file blobs => {(sum(file_sizes))}")

        controller_result = {}
        for automation in self.automations:
            results: Tuple[TransferDTO] = automation.run_automation(**kwargs)
            results = tuple(
                filter(
                    lambda r: r.start_time is not None and r.end_time is not None,
                    results,
                )
            )
            if self.debug:
                logger.debug(f"[{automation.__classname__}]" f"Results :: {results}")

            # filter results based on filemap
            results_filemapped = tuple(filter(lambda r: r.fname in filemap, results))

            # in case in some automation, fname are temp ids returned by
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
            logger.info(f"[{automation.__classname__}]" f"Throughput = {throughput}")
            controller_result[automation.__classname__] = {"throughput": throughput}

            self.generate_graph("tmp/", automation.__classname__, results)

        logger.info(
            f"Controller took total {time.time()-controller_start}" f"seconds to run!"
        )
        return controller_result

    def generate_graph(
        self, filename: TYPE_PATH, title: str, timesdto: Tuple[TransferDTO]
    ):
        """
        Generate (bar) graph for the transfer.
        Args:
            ```filename```: ```Union[str, pathlib.Path]```
                Path to save the figure
            ```title```: ```str```
                TItle for the graph (also used as filename)
            ```timesdto```: ```Tuple[TransferDTO]```
                List  of transfer information objects from which delta time
                is computed for the transfer.
        """
        if not MATPLOTLIB:
            logger.warning("Matplotlib not found. " + "Can't generate figure! Halting!")
            return

        times = self.dtotimes_to_times(timesdto)
        for i in range(len(times)):
            plt.barh(i + 1, times[i][1] - times[i][0], left=times[i][0])
        plt.title(title)

        path = pathlib.Path(filename)
        # if it's a directory, create a new unique filename
        if not path.as_posix().endswith((".png", ".jpg", ".jpeg")):
            path.mkdir(parents=True, exist_ok=True)
            path = path.joinpath(f"{title}_{time.time()}.png")
        plt.savefig(path)

    def caclulate_throughput(
        self, file_sizes: List[int], timesdto: Tuple[TransferDTO]
    ) -> float:
        """
        Calculate transfer throughput in terms of Gbps

        Args:
            ```file_sizes```: ```List[int]```
                List of file sizes to compute throughput
            ```timesdto```: ```Tuple[TransferDTO]```
                List  of transfer information objects
                from which delta times are calculated for throughput.
        Returns:
            ```float``` throughput value (Gbps)
        """
        times = self.dtotimes_to_times(timesdto)
        total_volume = sum(file_sizes)
        val = 0
        try:
            # *8 -> for Gbps not GBps
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
