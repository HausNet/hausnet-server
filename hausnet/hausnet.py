"""Entry point to clients to set up the network"""
from typing import Any, Dict

from hausnet.builders import PlantBuilder


class HausNet:
    """Give access to the network plant to clients"""
    def __init__(self, loop, mqtt_host: str, mqtt_port: int, config: Dict[str, Any]):
        self.device_interfaces = PlantBuilder(loop, mqtt_host, mqtt_port).build(config)

