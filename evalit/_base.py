#!/usr/bin/env python3

from __future__ import annotations

import copy
import random
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple, Type, Union

import yaml
from loguru import logger

from .structures import TYPE_PATH, TransferDTO


class AbstractAutomation(ABC):
    """
    This is the Abstract Base Class that represents any automation component.

    The automation component works by using configuration for
    source s3 and destination s3 buckets.

    Some of the implementation are:
        - `rclone.rclone_automation.RcloneAutomation`
        - `nifi.nifi_automation.NifiAutomation`
        - `mft.mft_automation.MFTAutomation`

    Each automation component should implement `rclone_automation` method
    and must return `Tuple[TransferDTO]` data structure.
    """

    _CFG_KEYS = {
        "src": [
            "source_token",
            "source_secret",
            "source_s3_endpoint",
            "source_s3_bucket",
            "source_s3_region",
        ],
        "dest": [
            "dest_token",
            "dest_secret",
            "dest_s3_endpoint",
            "dest_s3_bucket",
            "dest_s3_region",
        ],
    }

    def __init__(
        self,
        config: Union[TYPE_PATH, Dict[str, str]],
        files: Optional[Sequence[TYPE_PATH]] = None,
        debug: bool = False,
        **kwargs,
    ) -> None:
        """
        Args:
            `config`: `str` or `pathlib.Path` or `dict`
                Represents the yaml config for source and destination.
                If `str` or `pathlib.Path`, yaml path has to be provided.

            `files`: `List[str]`
                List of filenames for transfer

            debug: `bool`
                Flag to represent debugging mode.
        """
        assert isinstance(debug, bool)
        self.debug = bool(debug)

        files = files or []
        self.files = tuple(files)

        if isinstance(config, str):
            config = self.load_yaml(config)
        if not isinstance(config, dict):
            raise TypeError(
                f"Invalid type for config. Expected Dict[str, str]. Got {type(config)}"
            )
        self.config = copy.deepcopy(config)
        if debug:
            logger.debug(self.__get_redacted_cfg())
        self._sanity_check_config(self.config)

    @abstractmethod
    def run_automation(self, **kwargs) -> Tuple[TransferDTO]:
        """
        Main method to start transfer
        """
        raise NotImplementedError()

    @staticmethod
    def load_yaml(config_yaml: TYPE_PATH) -> Dict[str, str]:
        """
        Load config from yaml file!
        """
        logger.info(f"Loading yaml from {config_yaml}")
        if not isinstance(config_yaml, (str, Path)):
            raise TypeError(
                f"Invalid type for config_yaml={config_yaml}. Expected type of str or pathlib.Path. Got {type(config_yaml)}"
            )

        config = {}
        with open(config_yaml) as f:
            # TODO: Use better loader
            config = yaml.safe_load(f)
        return config

    def _sanity_check_config(self, cfg: Dict[str, str]) -> bool:
        """
        Check if we have sufficient keys in the config dict.
        """
        if self.debug:
            logger.debug(f"cfg => {cfg}")
        for d_type in ["src", "dest"]:
            for src_key in self._CFG_KEYS.get(d_type, []):
                if src_key not in cfg:
                    raise ValueError(f"key={src_key} not found in the config!")
        return True

    @property
    def __classname__(self) -> str:
        return self.__class__.__name__

    def __get_redacted_cfg(self) -> Dict[str, str]:
        """
        Returns the stored config in redacted form.
        """

        def _redact_string(text, nchars=7):
            n = len(text)
            nchars = nchars or 0
            nchars = min(nchars, n)
            indices = random.choices(list(range(n)), k=nchars)
            return "".join(["*" if i in indices else c for i, c in enumerate(text)])

        res = copy.deepcopy(self.config)
        res = map(lambda x: (x[0], _redact_string(x[1])), res.items())
        return dict(res)

    def __str__(self) -> str:
        params = copy.deepcopy(self.__dict__)
        params.pop("config", None)
        params.pop("files", None)
        return f"[{self.__classname__}] | [Redacted config] = {self.__get_redacted_cfg()} | [params] => {params}"


class AbstractController(ABC):
    """
    This component encapsulates all the automation component.
    The `run(...)` method is used for computing downstream tasks like:
        - calculating throughput
        - generating graphs
    """

    def __init__(
        self,
        automations: Optional[Tuple[Type[AbstractAutomation]]] = None,
        debug: bool = False,
    ) -> None:
        automations = automations or tuple()
        self.sanity_check_automations(automations)
        self.automations = automations
        self.debug = bool(debug)

    @abstractmethod
    def run(self, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def get_source_file_map(cfg: TYPE_PATH) -> Dict[str, dict]:
        """
        A helper method to get all the available files in the source.

        Returns:
            A dictionary mapping from filename to file metadata.
            The `size` metadata is in GB (GigaBytes).

            Example:

                .. code-block:: python

                    {"testfile": {"size": 0.9}}



        """
        if isinstance(cfg, str):
            cfg = AbstractAutomation.load_yaml(cfg)

        # importing at runtime, as it's not a necessity to use this function
        import boto3

        logger.debug(f"Boto3 version: {boto3.__version__}")

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=cfg["source_token"],
            aws_secret_access_key=cfg["source_secret"],
            endpoint_url=cfg["source_s3_endpoint"],
        )
        response_contents = s3_client.list_objects_v2(
            Bucket=cfg["source_s3_bucket"]
        ).get("Contents", {})
        return {
            val["Key"]: dict(size=val["Size"] / (1024 * 1024 * 1024))
            for val in response_contents
        }

    def sanity_check_automations(
        self, automations: Tuple[Type[AbstractAutomation]]
    ) -> bool:
        for automation in automations:
            if not isinstance(automation, AbstractAutomation):
                raise TypeError(
                    f"Invalid type for automation={automation}. Expected Type[AbstractAutomation]. Got {type(automation)}"
                )
        return True

    def add_automation(
        self, automation: Type[AbstractAutomation]
    ) -> Type[AbstractController]:
        """
        Add single automation
        """
        if not isinstance(automation, AbstractAutomation):
            raise TypeError(
                f"Invalid type for automation. Expected Type[AbstractAutomation]. Got {type(automation)}"
            )
        self.automations += (automation,)
        return self

    def add_automations(
        self, automations: Tuple[Type[AbstractAutomation]]
    ) -> Type[AbstractController]:
        """
        Add multiple automations
        """
        for automation in automations:
            self = self.add_automation(automation)
        return self

    def __str__(self) -> str:
        res = "\n".join([str(automation) for automation in self.automations])
        return f"{self.__classname__} | automations ==>\n{res}"

    def __repr__(self) -> str:
        return str(self)

    @property
    def __classname__(self) -> str:
        return self.__class__.__name__
