from queue import Empty
from typing import Dict, Any, List
import asyncio
import logging
import queue

import paho.mqtt.client as mqttc
from aioreactive.core import AsyncStream, subscribe

from hausnet.config import conf

logger = logging.getLogger(__name__)

# The namespace prefix for all topics
TOPIC_NAMESPACE = 'hausnet/'

TOPIC_UPSTREAM_APPENDIX = '/upstream'
TOPIC_DOWNSTREAM_APPENDIX = '/downstream'
TOPIC_DIRECTION_UPSTREAM = 1
TOPIC_DIRECTION_DOWNSTREAM = 2

# The topics to be subscribed to - e.g. hausnet/sonoff_switch/ABC123/upstream
TOPICS_SUBSCRIBED_TO = f"{TOPIC_NAMESPACE}+/+{TOPIC_UPSTREAM_APPENDIX}"


def topic_name(prefix, direction: int):
    """Create a topic name from a prefix and a direction. E.g. 'node/AAA111/upstream' or 'node/BBB222/downstream"""
    if direction == TOPIC_DIRECTION_UPSTREAM:
        return f"{prefix}{TOPIC_UPSTREAM_APPENDIX}"
    return f"{prefix}{TOPIC_DOWNSTREAM_APPENDIX}"


class AsyncStreamFromQueue(AsyncStream):
    """An AsyncStream streaming from an async Queue"""
    def __init__(self, loop, source_queue: asyncio.Queue) -> None:
        super().__init__()
        self.queue: asyncio.Queue = source_queue
        self.stream_task = loop.create_task(self.stream())

    async def stream(self):
        while True:
            logger.debug("Awaiting message from queue...")
            message = await self.queue.get()
            self.queue.task_done()
            await self.asend(message)
            logger.debug("Sent message from queue: %s", str(message))


class AsyncStreamToQueue(AsyncStream):
    """An async stream that dumps the values it observes into an async queue."""
    def __init__(self, sink_queue: asyncio.Queue) -> None:
        super().__init__()
        self.queue = sink_queue

    async def asend(self, value) -> None:
        logger.debug("Received message, putting in queue: message=%s.", str(value))
        await self.queue.put(value)


class MessageStream:
    """Encapsulates one-directional data flow between the API to and the MQTT network."""

    def __init__(self, loop, source: AsyncStreamFromQueue, stream_ops: AsyncStream, sink: AsyncStreamToQueue) -> None:
        """Sets up subscribing the given stream to the given sink via an async task.

        :param stream_ops: An async stream with a source and chained operations.
        :param sink:       An async stream that dumps its received values into a Queue (meant for the MQTT client).
        """
        self.source = source
        self.stream_ops = stream_ops
        self.sink = sink
        self.out_task = loop.create_task(self._subscribe())

    async def _subscribe(self) -> None:
        """Subscribe the exit async stream to the input"""
        logger.debug("Subscribing the downstream sink to the down stream...")
        await subscribe(self.stream_ops, self.sink)


class MqttClient(mqttc.Client):
    """ Manages MQTT communication for the HausNet environment. Constrains the Paho client to just those
    functions needed to support the needed functionality.
    """

    def __init__(
            self,
            pub_queue: queue.Queue,
            sub_queue: queue.Queue,
            host: str = conf.MQTT_BROKER,
            port: int = conf.MQTT_PORT
    ) -> None:
        """ Set up the MQTT connection and support for streams.

            :param pub_queue: Synchronous queue for messages to be sent
            :param sub_queue: Synchronous queue for received messages
            :param host: Host device_id of broker.
            :param port: Port to use, defaults to standard.
        """
        super().__init__()
        self.pub_queue: queue.Queue = pub_queue
        self.sub_queue: queue.Queue = sub_queue
        self.host: str = host
        self.port: int = port
        logger.info("Connecting to MQTT: host=%s; port=%s", host, str(port))
        self.connected = False
        self.connect(host, port)
        self.loop_start()

    def loop(self, timeout: float = 1.0, max_packets: int = 1):
        """Override the parent class to check for new messages. If there are messages in the upstream queue,
        publish them all, then let the parent's loop() have a go
        """
        if self.connected:
            self._publish_from_queue()
        return super().loop(timeout, max_packets)

    def _publish_from_queue(self) -> None:
        """Publish all the messages available in the publish queue"""
        while not self.pub_queue.empty():
            try:
                message = self.pub_queue.get(False)
                self.publish(message['topic'], message['message'])
            except Empty:
                return

    def _subscribe_to_network(self):
        """Subscribes to all topics nodes in the network will publish to"""
        logger.debug("Subscribing to topic(s): %s", TOPICS_SUBSCRIBED_TO)
        self.subscribe(TOPICS_SUBSCRIBED_TO)

    # noinspection PyUnusedLocal,PyMethodOverriding
    def on_connect(self, client: mqttc.Client, user_data: Dict[str, Any], flags: Dict[str, Any], rc: str):
        """On connection failure, reconnect"""
        if rc == mqttc.CONNACK_ACCEPTED:
            logger.info("MQTT connected.")
            self.connected = True
            self._subscribe_to_network()
            return
        logger.error("MQTT connection failed: code=%s; text=%s. Retrying...", str(rc), mqttc.connack_string(rc))
        self.connected = False
        self.reconnect()

    # noinspection PyUnusedLocal,PyMethodOverriding
    def on_disconnect(self, client: mqttc.Client, user_data: Dict[str, Any], rc: str):
        """Just set the manager's connected flag and log the reason"""
        logger.error(
            "MQTT unexpectedly disconnected: code=%s; text=%s. Retrying...",
            str(rc),
            mqttc.connack_string(rc)
        )
        self.connected = False
        self.reconnect()

    # noinspection PyUnusedLocal,PyMethodOverriding
    def on_message(self, client: mqttc.Client, user_data: Dict[str, Any], message: mqttc.MQTTMessage):
        """Called when a message is received on a subscribed-to topic. Places the topic + message in the up stream"""
        logger.debug("Message received: topic=%s; message=%s", message.topic, message.payload)
        self.sub_queue.put_nowait({'topic': message.topic, 'message': message.payload})

    # noinspection PyUnusedLocal,PyMethodMayBeStatic,PyMethodOverriding
    def on_subscribe(self, client: mqttc.Client, user_data: Dict[str, Any], mid: Any, granted_qos: List[int]):
        """Called when subscription succeeds"""
        logger.debug("Subscription succeeded")
