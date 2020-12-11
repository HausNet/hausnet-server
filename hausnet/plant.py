""" Support for a plant (analogous to an industrial plant) that consists of a network of devices. """

import asyncio
from typing import cast, Dict
import queue

import janus

from hausnet.config import conf
from hausnet.devices import Device, CompoundDevice
from hausnet.flow import MessagePipe, AsyncStreamFromQueue, AsyncStreamToQueue, MqttClient


class DeviceAssembly:
    """ A class binding together everything needed to work with a device: The device itself; Its upstream and
        downstream data pipes.

        TODO: When constructing, make both ends of pipes Janus queues so either sync/async access is possible.
    """

    def __init__(
            self,
            device: (Device, CompoundDevice),
            upstream_pipe: MessagePipe = None,
            downstream_pipe: MessagePipe = None
    ) -> None:
        """ Set up the components

            :param device:      The device object, capturing the static structure of the device and its owner / sub-devices
            :param upstream_pipe:   A MessagePipe managing upstream data flow
            :param downstream_pipe: A MessagePipe managing downstream data flow
        """
        self.device: (Device, CompoundDevice) = device
        self.upstream_pipe: MessagePipe = upstream_pipe
        self.downstream_pipe: MessagePipe = downstream_pipe
        # Convenience accessors to in- and out-queues, from a client perspective
        self.client_in_queue: asyncio.Queue = downstream_pipe.source.queue if downstream_pipe else None
        self.client_out_queue: asyncio.Queue = upstream_pipe.sink.queue if upstream_pipe else None


class DevicePlant:
    """ A plant containing devices, the network connecting them, and providing the interfaces to the external
        world.
    """

    def __init__(
            self,
            loop: asyncio.BaseEventLoop,
            mqtt_server: str = conf.MQTT_BROKER,
            mqtt_port: int = conf.MQTT_PORT
    ) -> None:
        self.loop = loop
        # The MQTT interface queues, from the plant side of things.
        self.upstream_src_queue = janus.Queue()
        self.downstream_dest_queue = janus.Queue()
        # Convenience accessors for interface queue sink / source
        self.upstream_source = AsyncStreamFromQueue(cast(asyncio.Queue, self.upstream_src_queue.async_q))
        self.downstream_sink = AsyncStreamToQueue(cast(asyncio.Queue, self.downstream_dest_queue.async_q))
        # The MQTT client
        self.mqtt_client: MqttClient = MqttClient(
            cast(queue.Queue, self.downstream_dest_queue.sync_q),
            cast(queue.Queue, self.upstream_src_queue.sync_q),
            mqtt_server,
            mqtt_port
        )
        # The device assemblies in the plant
        self.device_assemblies: Dict[str, DeviceAssembly] = {}

    def start(self):
        """ Start up all the pipes by starting streaming from the source queues in both directions. """
        assembly: DeviceAssembly
        self.upstream_source.start()
        for assembly in self.device_assemblies.values():
            if assembly.downstream_pipe is None:
                continue
            assembly.downstream_pipe.source.start()

    def stop(self):
        """ Stop all the pipes by stopping streaming from the source queues in both directions. """
        assembly: DeviceAssembly
        self.upstream_source.stop()
        for assembly in self.device_assemblies.values():
            if assembly.downstream_pipe is None:
                continue
            assembly.downstream_pipe.source.stop()

