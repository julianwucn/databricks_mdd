import yaml
from pathlib import Path

class _Environment:
    """Singleton environment config that auto-loads from YAML."""

    def __init__(self):
        self._loaded = False

    def _load(self):
        self.root_path_metadata = Path(__file__).parent.parent / "etl" / "metadata"
        config_path = self.root_path_metadata / "environment.yml"
        if not config_path.exists():
            raise FileNotFoundError(f"Missing config: {config_path}")

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        for key, value in config.items():
            setattr(self, key, value)

        self._loaded = True

    def __getattr__(self, item):
        if not self._loaded:
            self._load()
        return self.__dict__.get(item)

# This is the public interface
Environment = _Environment()
