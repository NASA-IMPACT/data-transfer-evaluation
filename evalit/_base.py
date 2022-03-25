#!/usr/bin/env python3

from __future__ import annotations

import copy
import random
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional, Sequence, Type

import yaml
from loguru import logger

from .structures import TYPE_PATH


class AbstractAutomation(ABC):
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
        config: Dict[str, str],
        files: Optional[Sequence[TYPE_PATH]] = None,
        debug: bool = False,
    ) -> None:
        assert isinstance(debug, bool)
        self.debug = bool(debug)

        files = files or []
        self.files = tuple(files)

        if not isinstance(config, dict):
            raise TypeError(
                f"Invalid type for config. Expected Dict[str, str]. Got {type(config)}"
            )
        self.config = copy.deepcopy(config)
        self._sanity_check_config(self.config)

    @classmethod
    def from_yaml(
        cls,
        cfg_yaml: TYPE_PATH,
        files: Optional[Sequence[TYPE_PATH]] = None,
        debug: bool = False,
    ) -> Type[AbstractAutomation]:
        """
        Allows initialization with yaml configuration.
        """
        cfg = cls.load_yaml(cfg_yaml)
        return cls(config=cfg, files=files, debug=debug)

    # @abstractmethod
    def run_automation(self, **kwargs):
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
        return (
            f"[{self.__classname__}] | [Redacted config] = {self.__get_redacted_cfg()}"
        )
