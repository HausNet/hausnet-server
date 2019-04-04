import asyncio
import socket
import os
import time
import logging
import threading
from collections import AsyncIterable, AsyncIterator
from typing import Callable, Dict, Any
import paho.mqtt.client as mqttc
from aioreactive.abc import AsyncObserver, AsyncDisposable
from aioreactive.core import AsyncStream, AsyncObservable
from aioreactive.core.observables import T

from hausnet.config import conf
import hausnet.coder as coder

logger = logging.getLogger(__name__)


class ReceivedMessageQueue(AsyncIterable):
    def __init__(self):
        super().__init__()
        self.queue = asyncio.Queue()
        self.test_message_count = 0
        self.test_message_limit = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        print("Iterator called. Message queue length: " + str(self.queue.qsize()))
        if self.test_message_limit != 0:
            if self.test_message_count >= self.test_message_limit:
                raise StopAsyncIteration()
            else:
                self.test_message_count += 1
        return await self.queue.get()


class BufferedAsyncStream(AsyncStream):
    """ An AsyncStream with an internal buffer
    """
    def __init__(self) -> None:
        super().__init__()
        self.queue = asyncio.Queue()
        self.loop_running = False
        loop = asyncio.get_event_loop()
        self.sending_task = loop.create_task(self.send_from_queue())

    async def asend(self, value: T) -> None:
        """ Put a value in the queue for sending when ready
        """
        await self.queue.put(value)

    async def send_from_queue(self):
        while True:
            message = await self.queue.get()
            await super().asend(message)


class MqttClient(mqttc.Client):
    """ Manages MQTT communication for the HausNet environment. Constrains the Pentaho client to just those
        functions needed to support the needed functionality.
    """

    def __init__(self, host: str = conf.MQTT_BROKER, port: int = conf.MQTT_PORT):
        """ Initializes client.

            :param host:     Host name of broker.
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
        self.upstreamQueue = ReceivedMessageQueue()
        self.upstreamSource = AsyncObservable.from_async_iterable(self.upstreamQueue)

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
    def __init__(self, coder: coder.Coder):
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
        decoded_obj = self.coder.decode(message)
        self.listener(topic, decoded_obj)

    def publish(self, topic: str, obj: Any):
        """ Publish an encoded object to a topic. Encoding is as determined by the encoder set for this class
            :param topic: Topic to publish to
            :param obj: Object to decode & publish
        """
        self.mqtt_client.publish(topic, self.coder.encode(obj))


