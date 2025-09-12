import yaml
from pathlib import Path

BASE_DIR = Path("/usr/local/WOC")

class GetConfiguration:
    def __init__(self):
        config_file = BASE_DIR / "conf" / "config.yaml"
        with open(config_file, "r") as file:
            self.config_data = yaml.safe_load(file)
