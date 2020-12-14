"""Entry point to clients to set up the network"""
from typing import Any, Dict

from hausnet.builders import PlantBuilder
from hausnet.plant import DeviceAssembly


class HausNet:
    """Give access to the network plant to clients"""
    def __init__(self, loop, mqtt_host: str, mqtt_port: int, config: Dict[str, Any]):
        self.plant = PlantBuilder().build(config, loop, mqtt_host, mqtt_port)

    def device_assemblies(self) -> Dict[str, DeviceAssembly]:
        """ Convenience accessor to devices. """
        return self.plant.device_assemblies

    def start(self):
        """ Convenience accessor to plant startup. """
        self.plant.start()

