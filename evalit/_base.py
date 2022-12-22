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

    Each automation component should implement `rclone_automation(...)` method
    and must return `Tuple[TransferDTO]` data structure.

    Args:
        ```config```: ```Union[str, pathlib.Path, dict]```
            The input yaml configuration for the transfer.
            If path, then it will load the config from yaml loader
            Else, incoming dictionary is used as the config

        ```files```: ```Optional[Union[str, Path]]```
            List of files to benchmark.
            If provided, only these files will be used.
            Else, all the files from the source bucket are used
            (Defaults to None)

        ```debug```: ```bool```
                Flag used for debugging purpose.
                Defaults to False

    Example Structure of YAML should be:

        .. code-block: yaml

                ---
                source_token: "<minio_username / aws_access_key_id>"
                source_secret: "<minio_password / aws_secret_access_key>"
                source_s3_endpoint: "http://127.0.0.1:8080"
                source_s3_bucket: "src"
                source_s3_region: "us-east-1"

                dest_token: "<username>"
                dest_secret: "<password>"
                dest_s3_endpoint: "http://127.0.0.1:8090"
                dest_s3_bucket: "dest"
                dest_s3_region: "us-east-1"
    """

    # these keys are used for validation of incoming config
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
            ```config```: ```Union[str, pathlib.Path, dict]```
                The input yaml configuration for the transfer.
                If path, then it will load the config from yaml loader
                Else, incoming dictionary is used as the config

            ```files```: ```Optional[Union[str, Path]]```
                List of files to benchmark.
                If provided, only these files will be used.
                Else, all the files from the source bucket are used
                (Defaults to None)

            ```debug```: ```bool```
                    Flag used for debugging purpose.
                    Defaults to False
        """
        assert isinstance(debug, bool)
        self.debug = bool(debug)

        files = files or []
        self.files = tuple(files)

        if isinstance(config, str):
            config = self.load_yaml(config)
        if not isinstance(config, dict):
            raise TypeError(
                (
                    f"Invalid type for config. Expected Dict[str, str]. "
                    f"Got {type(config)}"
                )
            )
        self.config = copy.deepcopy(config)
        if debug:
            logger.debug(self._get_redacted_cfg(self.config))
        self._sanity_check_config(self.config)

    @abstractmethod
    def run_automation(self, **kwargs) -> Tuple[TransferDTO]:
        """
        Main entrypoint/method to start transfer

        Args:
            ```kwargs```: ```Any```
                extra keyword arguments that can be used
                for the transfer process

        Returns:
            Tuple of `structures.TransferDTO` objects
        """
        raise NotImplementedError()

    @staticmethod
    def load_yaml(config_yaml: TYPE_PATH) -> Dict[str, str]:
        """
        Load config from yaml file!

        Args:
            ```config_yaml```: ```Union[str, pathlib.Path]```
                Path to YAML file to be loaded

        Returns:
            ```Dict[str, str]``` dictionary config

        Example of dictionary structure:

            .. code-block: python

                {
                    "source_token": <username>,
                    "source_secret": <password>,
                    "source_s3_endpoint": "http://127.0.0.1:<PORT_1>",
                    "source_s3_bucket": "src",
                    "source_s3_region": "us-east-1",
                    "dest_token": <username>,
                    "dest_secret": <password>,
                    "dest_s3_endpoint": "htt*://127.0.0.1:<PORT_2>",
                    "dest_s3_bucket": "dest",
                    "dest_s3_region": "us-east-1",
                }
        """
        logger.info(f"Loading yaml from {config_yaml}")
        if not isinstance(config_yaml, (str, Path)):
            raise TypeError(
                (
                    f"Invalid type for config_yaml={config_yaml}. "
                    "Expected type of str or pathlib.Path. "
                    f"Got {type(config_yaml)}"
                )
            )

        config = {}
        with open(config_yaml) as f:
            # TODO: Use better loader
            config = yaml.safe_load(f)
        return config

    def _sanity_check_config(self, cfg: Dict[str, str]) -> bool:
        """
        Check if we have sufficient keys in the config dict based on the
        `AbstractAutomation._CFG_KEYS` structure for keys.

        Args:
            ```cfg```: ```Dict[str, str]```
                The configuration dictionary for the transfer

        Returns:
            If everything is fine, it returns True else raise error

        Example of dictionary structure:

            .. code-block: python

                {
                    "source_token": "a****",
                    "source_secret": "***s**rd",
                    "source_s3_endpoint": "ht*p**/127.0**.*:80*0",
                    "source_s3_bucket": "***",
                    "source_s3_region": "u****s***",
                    "dest_token": "*d*i*",
                    "dest_secret": "p**s****",
                    "dest_s3_endpoint": "htt*://127.0.*.1:*0**",
                    "dest_s3_bucket": "**s*",
                    "dest_s3_region": "u*-e*s***",
                }
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

    @staticmethod
    def _get_redacted_cfg(cfg: Dict[str, str]) -> Dict[str, str]:
        """
        Returns the stored config in redacted form.

        Args:
            ```cfg```: ```Dict[str, str]```
                Configuratin dictionary

        Returns:
            `Dict[str, str]` dictionary of same structure, but redacted

        Example of dictionary structure:

            .. code-block: python

                {
                    "source_token": "a****",
                    "source_secret": "***s**rd",
                    "source_s3_endpoint": "ht*p**/127.0**.*:80*0",
                    "source_s3_bucket": "***",
                    "source_s3_region": "u****s***",
                    "dest_token": "*d*i*",
                    "dest_secret": "p**s****",
                    "dest_s3_endpoint": "htt*://127.0.*.1:*0**",
                    "dest_s3_bucket": "**s*",
                    "dest_s3_region": "u*-e*s***",
                }
        """

        def _redact_string(text, nchars=7):
            n = len(text)
            nchars = nchars or 0
            nchars = min(nchars, n)
            indices = random.choices(list(range(n)), k=nchars)
            return "".join(
                ["*" if i in indices else c for i, c in enumerate(text)]
            )  # mask

        res = copy.deepcopy(cfg)
        res = map(lambda x: (x[0], _redact_string(x[1])), res.items())
        return dict(res)

    def __str__(self) -> str:
        params = copy.deepcopy(self.__dict__)
        params.pop("files", None)
        cfg = params.pop("config", {})
        return (
            f"[{self.__classname__}] | "
            f"[Redacted config] = {self._get_redacted_cfg(cfg)} | "
            f"[params] => {params}"
        )


class AbstractController(ABC):
    """
    This component encapsulates all the automation
    component into a single container.

    The `run(...)` method is used for computing downstream tasks like:
        - calculating throughput
        - generating graphs

    Args:
        ```automations```: ```Optional[Tuple[Type[AbstractAutomation]]]```
            List of automation objects to be used for transfers.
            Each automation object is of base type `AbstractAutomation`
        ```debug```: ```bool```
            Flag for debugging mode
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
        """
        Main/entrypoint method to run the automation.
        It goes through all the available `Type[AbstractAutomation]` objects
        and perform the transfer, track files and benchmark.
        """
        raise NotImplementedError()

    @staticmethod
    def get_source_file_map(cfg: TYPE_PATH) -> Dict[str, dict]:
        """
        A helper method to get all the available files in the source.

        Args:
            ```cfg```: ```Union[str, pathlib.Path]```
                Path to cfg

        Returns:
            A dictionary mapping from filename to file metadata
            in the source bucket.

            The `size` metadata is in GB (GigaBytes).

            Example:

                .. code-block:: python

                    {"testfile": {"size": 0.9}}



        """
        if isinstance(cfg, str):
            cfg = AbstractAutomation.load_yaml(cfg)

        # importing at runtime, as it's not a necessity to use this function
        import boto3  # noqa

        logger.debug(f"Boto3 version: {boto3.__version__}")

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=cfg["source_token"],
            aws_secret_access_key=cfg["source_secret"],
            endpoint_url=cfg["source_s3_endpoint"],
            region_name=cfg["source_s3_region"],
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
        """
        Check if the automation objects are valid types.

        Args:
            ```automations```: ```Tuple[Type[AbstractAutomation]]```
                Tuple of automation objects to check

        Returns:
            If successful, returns True.
            Else, raises error.
        """
        for automation in automations:
            if not isinstance(automation, AbstractAutomation):
                raise TypeError(
                    (
                        f"Invalid type for automation={automation}. "
                        "Expected Type[AbstractAutomation]. "
                        f"Got {type(automation)}"
                    )
                )
        return True

    def add_automation(
        self, automation: Type[AbstractAutomation]
    ) -> Type[AbstractController]:
        """
        Add single automation to the controller pipeline.

        Args:
            ```automation```: ```Type[AbstractAutomation]```
                An automation object to add to the controller pipeline

        Returns:
            Returns the self (controller) object itself.
        """
        if not isinstance(automation, AbstractAutomation):
            raise TypeError(
                (
                    "Invalid type for automation. "
                    "Expected Type[AbstractAutomation]. "
                    f"Got {type(automation)}"
                )
            )
        self.automations += (automation,)
        return self

    def add_automations(
        self, automations: Tuple[Type[AbstractAutomation]]
    ) -> Type[AbstractController]:
        """
        Add multiple automations to the controller pipeline.

        Args:
            ```automations```: ```Tuple[Type[AbstractAutomation]]```
                Tuple of the automation objects to add

        Returns:
            Returns the self (controller) object itself.
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
