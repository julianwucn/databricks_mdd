import yaml
import json
import inspect
import logging
from pathlib import Path
from mdd.utils import DecoratorUtil
from mdd.environment import Environment

@DecoratorUtil.add_logger()
class Metadata:
    logger: logging.Logger
    def __init__(self, path, debug=False):
        self.path = Path(f"{Environment.root_path_metadata}/{path}")
        self.debug = debug
        self._config = self._load_yaml()

    #@DecoratorUtil.log_function(), do not add this because it is called by init
    def _load_yaml(self):
        if not self.path.exists():
            message = f"YAML file not found: {self.path}"
            self.logger.error(message)
            raise FileNotFoundError(message)

        with self.path.open("r") as file:
            try:
                return yaml.safe_load(file)
            except yaml.YAMLError as e:
                message = f"Failed to parse YAML: {e}"
                self.logger.error(message)
                raise ValueError(message)

    @DecoratorUtil.log_function()
    def get(self, *keys, default=None):
        """
        Fetch nested config values. Example:
        reader.get("database", "host")
        """
        value = self._config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    @DecoratorUtil.log_function()
    def as_dict(self):
        """Return the full config as a dictionary"""
        return self._config

    @DecoratorUtil.log_function()
    def to_json(self, indent=2):
        """Convert the YAML content to a JSON string"""
        return json.dumps(self._config, indent=indent)
