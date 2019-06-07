"""Entry point to clients to set up the network"""
from typing import Any, Dict

from hausnet.builders import DevicePlantBuilder
from hausnet.flow import SyncToAsyncBufferedStream, MqttClient, AsyncToSyncBufferedStream


class HausNet:
    """Give access to the network plant to clients"""
    def __init__(self, loop, mqtt_host: str, mqtt_port: int, config: Dict[str, Any]):
        upstream_src = SyncToAsyncBufferedStream(loop)
        downstream_sink = AsyncToSyncBufferedStream(loop)
        self.mqtt_client = MqttClient(
            upstream_src,
            downstream_sink,
            mqtt_host,
            mqtt_port
        )
        self.device_bundles = DevicePlantBuilder(upstream_src, downstream_sink).build(config)

