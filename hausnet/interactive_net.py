##
# Tool to interactively test HausNet networks
#
import asyncio

from aioreactive.core import subscribe, AsyncAnonymousObserver

from hausnet.builders import DevicePlantBuilder
from hausnet.flow import BufferedAsyncStream, MqttClient
from hausnet.config import conf


def command_loop():
    loop = asyncio.get_event_loop()
    up_stream = BufferedAsyncStream(loop)
    bundles = DevicePlantBuilder(up_stream).build(conf.HAUSNET_CONFIG)
    mqtt_client = MqttClient(up_stream)

    async def sink_to_stdout(message):
        print(message)

    async def main():
        for name, device_bundle in bundles.items():
            await subscribe(device_bundle.up_stream, AsyncAnonymousObserver(sink_to_stdout))
        await mqtt_client.up_stream.stream_from_queue()

    loop.run_until_complete(main())
    loop.close()


if __name__ == '__main__':
    command_loop()
