from typing import Callable, Dict, Any
import os
import time
import socket
import threading
import asyncio
import logging

import paho.mqtt.client as mqttc
from aioreactive.core import AsyncStream, AsyncObserver
from aioreactive.core.observables import T
import janus

from hausnet.config import conf
import hausnet.coders as coders

logger = logging.getLogger(__name__)


class BufferedAsyncSource(AsyncStream):
    """ An AsyncStream with an internal buffer, that accepts data synchronously, and sends data asynchronously.
    """
    def __init__(self) -> None:
        super().__init__()
        self.loop = asyncio.get_event_loop()
        self.queue = janus.Queue(loop=self.loop)
        self.sending_task = self.loop.create_task(self.stream_from_queue())

    async def stream_from_queue(self):
        while True:
            await self.send_from_queue()

    async def send_from_queue(self):
        message = await self.queue.async_q.get()
        await self.asend(message)
        self.queue.async_q.task_done()

    def buffer(self, value: T):
        """ Place a value in the buffer
        """
        self.queue.sync_q.put_nowait(value)


class TestableBufferedAsyncSource(BufferedAsyncSource):
    """ A buffered stream for testing that will only transmit a certain number of messages before stopping
    """
    def __init__(self, max_messages: int = 0) -> None:
        super().__init__()
        self.max_messages = max_messages

    async def stream_from_queue(self):
        message_count = 0
        while True:
            await self.send_from_queue()
            message_count += 1
            if message_count >= self.max_messages:
                self.queue.close()
                break


class TappedStream(AsyncStream):
    """ Allows 'tapping' a stream - exposing values for storage / state purposes without interrupting the flow.
        Intended to allow devices to take actions on state changes outside of the main data flow.
    """
    def __init__(self, tap_func: Callable[[T], None]) -> None:
        super().__init__()
        self.tap_func = tap_func

    async def asend(self, value: T) -> None:
        self.tap_func(value)
        await super().asend(value)


class MqttClient(mqttc.Client):
    """ Manages MQTT communication for the HausNet environment. Constrains the Pentaho client to just those
        functions needed to support the needed functionality.
    """

    def __init__(self, host: str = conf.MQTT_BROKER, port: int = conf.MQTT_PORT):
        """ Initializes client.

            :param host:     Host device_id of broker.
            :param port:     Port to use, defaults to standard.
        """
        super().__init__()
        self.host = host
        self.port = port
        self.listener = None
        self.listenerLock = threading.Lock()
        self.connected = False
        self.on_connect = self.connect_cb
        self.on_disconnect = self.disconnect_cb
        self.on_subscribe = self.subscribe_cb
        self.on_message = self.message_cb
        #self.upstreamQueue = BufferedAsyncSource()
        #self.upstreamSource = AsyncObservable.from_async_iterable(self.upstreamQueue)

    def set_listener(self, listener: Callable[[str, str], None]):
        """ Set the function that should be called on receipt of messages on subscribed topics. Both the topic and
            message payload will be forwarded to this function.

            :param listener: The receiver of _all_ messages
        """
        self.listener = listener

    def subscribe(self, topic, qos=0):
        """ Test that a listener has been set before calling the parent function.

            :raises: An exception when a listener has not yet been set.
        """
        if not self.listener:
            raise Exception("No listener set, refusing to subscribe to topic: {}".format(topic))
        super().subscribe(topic)

    def run(self):
        """ Connect to broker, then subscribe to topics.

            TODO: Use thread event instead of sleep loop
            TODO: Avoid long startup times by backgrounding connection on another thread / asyncio
        """
        self.loop_start()
        self.connect(self.host, self.port)
        while not self.connected:
            logging.debug("Waiting for connection...")
            time.sleep(0.1)

    async def send_upstream(self, packet):
        self.upstreamQueue.queue.put_nowait(packet)
        #await self.upstreamSource.asend(packet)

    def handle_received_msg(self, topic: str, message: str):
        """ Forward a message received on a topic to the appropriate listener
        """
        self.listenerLock.acquire()
        if self.listener:
            self.listener(topic, message)
        else:
            logger.error("No listener set, message discarded. Topic: %s; Message: %s", topic, message)
        self.listenerLock.release()

        # New method. TODO: Make thread-safe because delivery happens from other thread
        asyncio.ensure_future(self.send_upstream({'topic': topic, 'message': message}))

    @staticmethod
    def client_id():
        """ Generate a client ID
        """
        return "%s/hausnet/%d" % (socket.gethostname(), os.getpid())

    # noinspection PyUnusedLocal
    def connect_cb(self, client: mqttc.Client, user_data: Dict[str, Any], flags: Dict[str, Any], rc: str):
        """ On connection, subscribe to registered listeners, and handle errors (TBD)
        """
        if rc != mqttc.CONNACK_ACCEPTED:
            logger.critical("Connection failed with code %s: %s", rc, mqttc.connack_string(rc))
            return
        self.connected = True

    # noinspection PyUnusedLocal
    def disconnect_cb(self, client: mqttc.Client, user_data: Dict[str, Any], rc: str):
        """ Just set the manager's connected flag and log the reason
        """
        if rc != mqttc.MQTT_ERR_SUCCESS:
            logger.critical("Unexpected disconnection, code: %s", rc)
        self.connected = False

    # noinspection PyUnusedLocal
    def message_cb(self, client: mqttc.Client, user_data: Dict[str, Any], message: mqttc.MQTTMessage):
        """ Called when a message is received on a subscribed-to topic. Relays it to the appropriate listener
        """
        logger.debug("Message received on topic %s: %s", message.topic, message.payload)
        self.handle_received_msg(message.topic, message.payload.decode('ASCII'))

    # noinspection PyUnusedLocal
    @staticmethod
    def subscribe_cb(client: mqttc.Client, user_data: Dict[str, Any], mid, granted_qos):
        """ Just log the fact that a subscription happened.
        """
        logger.debug("Subscribed, message ID: %s", mid)


class MessageCoder:
    mqtt_client = MqttClient()

    """ Manages encoded messages on top of MQTT. The specific encoding is specified by the encoder and decoder
        e.g. JSON vs protocol buffers, are passed in the constructor.
    """
    def __init__(self, coder: coders.Coder):
        self.mqtt_client.set_listener(self.forward_decoded_message)
        self.coder = coder
        self.listener = None
        self.listenerLock = threading.Lock()

    def set_listener(self, listener: Callable[[str, Dict[str, Any]], None]):
        """ Register a callable, accepting a string and a dictionary (topic & decoded message), as a listener to
            messages arriving on subscribed topic.
        """
        self.listenerLock.acquire()
        self.listener = listener
        self.listenerLock.release()

    def forward_decoded_message(self, topic: str, message: str):
        """ Gets called when the MQTT client receives a message. Decodes the message using the provided decoder. This
            (should) result in a dictionary representation of the underlying format.
        """
        if not self.listener:
            raise Exception("No listener defined yet. Topic: %s; Message: %s", topic, message)
        decoded_obj = self.coders.decode(message)
        self.listener(topic, decoded_obj)

    def publish(self, topic: str, obj: Any):
        """ Publish an encoded object to a topic. Encoding is as determined by the encoder set for this class
            :param topic: Topic to publish to
            :param obj: Object to decode & publish
        """
        self.mqtt_client.publish(topic, self.coders.encode(obj))

