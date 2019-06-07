from abc import ABC
from queue import Empty
from typing import Dict, Any, List, Callable
import asyncio
import logging

import paho.mqtt.client as mqttc
from aioreactive.core import AsyncStream, subscribe, AsyncAnonymousObserver, AsyncObservable
import janus

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


class FixedSizeStream(ABC):
    def __init__(self, max_messages):
        """Set up the counting of messages. Note that using this class requires the implementation or the inheritance
        of an implementation of asend()

        :param max_messages: The number of messages to send.
        """
        self.max_messages = max_messages

    async def stream(self):
        """Loop to stream values from the queue until max reached"""
        for message_count in range(0, self.max_messages):
            # noinspection PyUnresolvedReferences
            await self.asend_from_queue()


class FromBufferAsyncStream(AsyncStream):
    """An AsyncStream that streams from an async queue forever. In order for a message to be sent, put it in the queue
    of this class
    """
    def __init__(self, loop):
        """Set up the queue to use"""
        super().__init__()
        self.queue = asyncio.Queue(loop=loop)

    async def stream(self):
        """Permanent loop to send data as it becomes available"""
        while True:
            await self.asend_from_queue()

    async def asend_from_queue(self):
        logger.debug(f"Getting message from queue...")
        message = await self.queue.get()
        await self.asend(message)
        self.queue.task_done()

    async def asend(self, value):
        logger.debug("Got value: %s", str(value))
        await super().asend(value)


class FixedFromBufferAsyncStream(FixedSizeStream, FromBufferAsyncStream):
    """An FromBufferAsyncStream that stop sending after a fixed number of messages"""
    def __init__(self, loop, max_messages: int):
        """Set up the queue to use"""
        # noinspection PyCallByClass
        FromBufferAsyncStream.__init__(self, loop)
        # noinspection PyCallByClass
        FixedSizeStream.__init__(self, max_messages)


class ToBufferAsyncStream(AsyncStream):
    """An AsyncStream that streams into an async queue forever. Messages can be retrieved via the queue member
    variable of this class.
    """
    def __init__(self, loop):
        """Set up the queue to use"""
        super().__init__()
        self.queue = asyncio.Queue(loop=loop)

    async def asend(self, value):
        logger.debug("Sending value to async queue: %s", str(value))
        await self.queue.put(value)


class SyncToAsyncBufferedStream(AsyncStream):
    """An AsyncStream with an internal buffer, that accepts data synchronously, and sends data asynchronously. Use this
    class' queue to access the streamed values
    """
    def __init__(self, loop):
        """Create a Janus (async <-> sync) queue for a buffer.

        :param loop: The async event loop for the Janus queue
        """
        super().__init__()
        self._janus_queue = janus.Queue(loop=loop)
        self.queue = self._janus_queue.sync_q

    async def stream(self) -> None:
        """Permanent loop to send data as it arrives"""
        while True:
            await self.send_from_queue()

    async def send_from_queue(self) -> None:
        message = await self._janus_queue.async_q.get()
        await self.asend(message)
        self._janus_queue.async_q.task_done()


class FixedSyncToAsyncBufferedStream(SyncToAsyncBufferedStream):
    """ A SyncToAsyncBufferedStream that will only transmit a certain number of messages before stopping. Intended for
    testing, where the stream should stop when test data has been sent.
    """
    def __init__(self, loop, max_messages: int = 0):
        """Set up the parent object, and set up counting of messages

        :param loop: The event loop
        :param max_messages: The number of messages to send.
        """
        super().__init__(loop)
        self.max_messages = max_messages
        self.message_count = 0

    async def stream(self):
        """Send max_messages number of messages from the queue, then stop streaming."""
        while True:
            await self.send_from_queue()
            self.message_count += 1
            if self.message_count >= self.max_messages:
                break


class AsyncToSyncBufferedStream(AsyncStream):
    """An async stream that collects what it observes in a Janus queue for pickup by a non-async (e.g. on another
    thread) queue. Note that it may be possible to implement as an observer only, but the stream metaphor still
    applies even if it is async on one side and sync on another.
    """
    def __init__(self, loop):
        """Sets up the Janus queue, and easy access to the sync queue which needs to be accessible for
        values to be picked up.
        """
        super().__init__()
        self._janus_queue = janus.Queue(loop=loop)
        self.queue = self._janus_queue.sync_q

    async def asend_core(self, value) -> None:
        """Send value to async queue for pickup on sync side"""
        await self._janus_queue.async_q.put(value)


class MqttInterfaceStreams:
    """Encapsulates the upstream and downstream functionality needed to shuffle messages between MQTT and the async
    up and down-streams
    """
    def __init__(self, loop):
        """Sets up the up/down-stream buffers, sink, source, and message pushing task"""
        self.upstream_src: AsyncStream = AsyncStream()
        self.upstream_queue: janus.Queue = janus.Queue(loop=loop)
        self.upstream_task: (asyncio.Task, None) = None
        self.downstream_sink: AsyncStream = AsyncStream()
        self.downstream_queue: janus.Queue = janus.Queue(loop=loop)

    async def stream_up(self):
        """Streams data upstream. Creates a task that awaits values on the async side from the Janus queue, and then
        sends them down the upstream_src operations stream via the AsyncStream at its head.
        """

        async def push_upstream():
            while True:
                message = await self.upstream_queue.async_q.get()
                self.upstream_src.asend(message)

        self.upstream_task = asyncio.create_task(push_upstream())

    async def stream_down(self):
        """Provide an endpoint observer that dumps arriving messages (via the downstream) into a Janus buffer
        to be picked up by the threaded MQTT publish loop
        """

        async def subscriber(message: Any):
            """Put the received messages into the async buffer queue"""
            await self.downstream_queue.async_q.put(message)

        await subscribe(self.downstream_sink, AsyncAnonymousObserver(subscriber))


class DeviceInterfaceStreams:
    """Encapsulates functionality to manage streaming at the device end"""
    def __init__(self, loop, up_stream: AsyncObservable, down_stream: AsyncStream):
        self.loop = loop
        self._up_stream: AsyncObservable = up_stream
        self._down_stream: AsyncStream = down_stream
        self.downstream_queue: asyncio.Queue = asyncio.Queue(loop=loop)
        self.upstream_queue: asyncio.Queue = asyncio.Queue(loop=loop)
        self.downstream_task = None

    async def stream_up(self):
        """Run the upstream communication. Upstream data is pushed into the upstream_src chain of operations by
        a downstream async task, and end results are deposited into the upstream_buffer queue for each device.
        """

        async def subscriber(message: Any):
            """Put the received messages into the async buffer queue"""
            await self.upstream_queue.put(message)

        await subscribe(self._up_stream, AsyncAnonymousObserver(subscriber))

    def stream_down(self):
        """Runs the downstream communication. Creates a task that awaits values from the downstream queue, and then
        sends them down the downstream_sink operations stream via the AsyncStream at its head.
        """

        async def push_downstream():
            while True:
                message = await self.downstream_queue.get()
                self._down_stream.asend(message)

        self.downstream_task = asyncio.create_task(push_downstream())


class MqttClient(mqttc.Client):
    """ Manages MQTT communication for the HausNet environment. Constrains the Pentaho client to just those
    functions needed to support the needed functionality.
    """

    def __init__(self, loop, host: str = conf.MQTT_BROKER, port: int = conf.MQTT_PORT):
        """ Set up the MQTT connection and support for streams.

            :param loop: The async event loop to run the streams functionality on.
            :param host: Host device_id of broker.
            :param port: Port to use, defaults to standard.
        """
        super().__init__()
        self.host: str = host
        self.port: int = port
        self.on_connect: Callable = self.connect_cb
        self.on_disconnect: Callable = self.disconnect_cb
        self.on_message: Callable = self.message_cb
        self.on_subscribe: Callable = self.subscribe_cb
        self.streams: MqttInterfaceStreams = MqttInterfaceStreams(loop)
        self.loop_start()
        logger.info("Connecting to MQTT: host=%s; port=%s", host, str(port))
        self.connected = False
        self.connect(host, port)

    def loop(self, timeout: float = 1.0, max_packets: int = 1) -> None:
        """Override the parent class to check for new messages. If there are messages in the upstream queue,
        publish them all, then let the parent's loop() have a go"""
        self._publish_from_queue()
        super().loop(timeout, max_packets)

    def _publish_from_queue(self) -> None:
        """Publish all the messages available in the downstream queue"""
        queue = self.streams.downstream_queue.sync_q
        while not queue.empty():
            try:
                message = queue.get(False)
                self.publish(message['topic'], message['message'])
            except Empty:
                return

    def subscribe_to_network(self):
        """Subscribes to all topics nodes in the network will publish to"""
        logger.debug("Connecting to topic: %s", TOPICS_SUBSCRIBED_TO)
        self.subscribe(TOPICS_SUBSCRIBED_TO)

    # noinspection PyUnusedLocal
    def connect_cb(self, client: mqttc.Client, user_data: Dict[str, Any], flags: Dict[str, Any], rc: str):
        """On connection failure, reconnect"""
        if rc == mqttc.CONNACK_ACCEPTED:
            logger.info("MQTT connected.")
            self.connected = True
            self.subscribe_to_network()
            return
        logger.error("MQTT connection failed: code=%s; text=%s. Retrying...", str(rc), mqttc.connack_string(rc))
        self.connected = False
        self.reconnect()

    # noinspection PyUnusedLocal
    def disconnect_cb(self, client: mqttc.Client, user_data: Dict[str, Any], rc: str):
        """Just set the manager's connected flag and log the reason"""
        logger.error(
            "MQTT unexpectedly disconnected: code=%s; text=%s. Retrying...",
            str(rc),
            mqttc.connack_string(rc)
        )
        self.connected = False
        self.reconnect()

    # noinspection PyUnusedLocal
    def message_cb(self, client: mqttc.Client, user_data: Dict[str, Any], message: mqttc.MQTTMessage):
        """Called when a message is received on a subscribed-to topic. Places the topic + message in the up stream"""
        logger.debug("Message received: topic=%s; message=%s", message.topic, message.payload)
        self.streams.upstream_queue.sync_q.put_nowait({'topic': message.topic, 'message': message.payload})

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def subscribe_cb(self, client: mqttc.Client, user_data: Dict[str, Any], mid: Any, granted_qos: List[int]):
        """Called when subscription succeeds"""
        logger.debug("Subscription succeeded")
