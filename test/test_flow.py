import unittest
import unittest.mock as mock
import subprocess
import time
import asyncio
from typing import List, Dict

from aioreactive.core import AsyncStream, subscribe, AsyncIteratorObserver, AsyncAnonymousObserver, AsyncObservable
from aioreactive.core import Operators as op
from aioreactive.operators import from_iterable
from hausnet import flow
from hausnet import coder
from hausnet import device
from hausnet.config import conf
from hausnet.flow import MqttClient, BufferedAsyncSource
from hausnet.helpers import TestableBufferedAsyncStream


def send_mqtt_message(topic: str, payload: str):
    """ Convenience function to send an MQTT message via the external mosquitto publication client.

        :param topic:   Where to send message to.
        :param payload: The actual message.
    """
    subprocess.check_call([
        'mosquitto_pub',
        '-h', conf.MQTT_BROKER,
        '-t', topic,
        '-m', payload
    ])


class MqttMessageSourceTests(unittest.TestCase):
    """ Test MqttMessageSource's behaviour as a buffering observable
    """
    message_log = []

    async def process_messages(self, client: MqttClient, msg_count: int):
        while True:
            packet = await asyncio.wait_for(client.upstreamQueue.queue.get(), 1)
            self.message_log.append(packet)
            if len(self.message_log) >= msg_count:
                break

    def test_has_iterable_queue(self):
        """ Test that the MqttClient's message receive queue is iterable
        """
        self.message_log = []

        client = MqttClient()
        client.upstreamQueue.queue.put_nowait({'topic': 'topic_1', 'message': 'my_message_1'})
        client.upstreamQueue.queue.put_nowait({'topic': 'topic_2', 'message': 'my_message_2'})
        client.upstreamQueue.queue.put_nowait({'topic': 'topic_3', 'message': 'my_message_3'})
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.process_messages(client, 3))
        loop.close()
        self.assertEqual(self.message_log[0], {'topic': 'topic_1', 'message': 'my_message_1'})
        self.assertEqual(self.message_log[1], {'topic': 'topic_2', 'message': 'my_message_2'})
        self.assertEqual(self.message_log[2], {'topic': 'topic_3', 'message': 'my_message_3'})

    def test_is_buffering(self):
        """ Test that the BufferedAsyncSource buffers messages
        """
        self.message_log = []

        async def sink(message):
            self.message_log.append(message)

        async def main():
            stream = TestableBufferedAsyncStream(3)
            await stream.asend({'topic': 'topic_1', 'message': 'my_message_1'})
            await stream.asend({'topic': 'topic_2', 'message': 'my_message_2'})
            await stream.asend({'topic': 'topic_3', 'message': 'my_message_3'})
            await subscribe(stream, AsyncAnonymousObserver(sink))

        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
        loop.close()
        self.assertEqual(self.message_log[0], {'topic': 'topic_1', 'message': 'my_message_1'})
        self.assertEqual(self.message_log[1], {'topic': 'topic_2', 'message': 'my_message_2'})
        self.assertEqual(self.message_log[2], {'topic': 'topic_3', 'message': 'my_message_3'})

    def test_behaves_as_observable(self):
        async def observe(message):
            self.message_log.append(message)

        async def main(source: AsyncObservable):
                await subscribe(source, AsyncAnonymousObserver(observe))

        self.message_log = []
        client = MqttClient()
        client.upstreamQueue.test_message_limit = 3
        client.upstreamQueue.queue.put_nowait({'topic': 'topic_1', 'message': 'my_message_1'})
        client.upstreamQueue.queue.put_nowait({'topic': 'topic_2', 'message': 'my_message_2'})
        client.upstreamQueue.queue.put_nowait({'topic': 'topic_3', 'message': 'my_message_3'})
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(client.upstreamSource))
        loop.close()
        self.assertEqual(self.message_log[0], {'topic': 'topic_1', 'message': 'my_message_1'})
        self.assertEqual(self.message_log[1], {'topic': 'topic_2', 'message': 'my_message_2'})
        self.assertEqual(self.message_log[2], {'topic': 'topic_3', 'message': 'my_message_3'})



class MqttClientTests(unittest.TestCase):
    """ Test the MQTT communications management
    """
    asyncResults = []

    def setUp(self):
        """ Clear the async result collection bucket
        """
        MqttClientTests.asyncResults = []

    @staticmethod
    def test_message_receipt():
        """ Test that messages sent are received
        """
        mqtt_manager = flow.MqttClient(conf.MQTT_BROKER)
        listener = mock.MagicMock()
        mqtt_manager.set_listener(listener)
        mqtt_manager.run()
        mqtt_manager.subscribe('test')
        send_mqtt_message('test', 'hello')
        i = 0
        while not listener.called and i < 100:
            time.sleep(0.1)
            i += 1
        listener.assert_called_with('test', 'hello')

    @staticmethod
    async def resultCatcher(value):
        MqttClientTests.asyncResults.append(value)

    @staticmethod
    async def upstream_flow(source: AsyncObservable):
        """ Test pipeline from command input down to the MQTT client
        """
        sink = AsyncAnonymousObserver(MqttClientTests.resultCatcher)
        await subscribe(source, sink)

    @staticmethod
    async def print_value(value):
        print(value)

    def test_upstream_flow(self):
        """ Test that incoming data is streamed reactively
        """
        mqtt_client = flow.MqttClient()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(MqttClientTests.upstream_flow(mqtt_client.upstreamSource))
        loop.close()
        self.assertEqual(len(self.asyncResults), 3, "Expected three results")
        self.assertIn({'topic': 'test', 'message': '{ "name": 1, "value": "some_value" }'}, self.asyncResults)
        self.assertIn({'topic': 'test', 'message': '{ "name": 2, "value": "other_value" }'}, self.asyncResults)
        self.assertIn({'topic': 'test2', 'message': '{ "name": 3, "value": "next_value" }'}, self.asyncResults)


class MessageCoderTests(unittest.TestCase):
    """ Test the structured message communications management
    """
    @staticmethod
    def test_json_receipt():
        """ Test that structured messages from JSON are received and decoded correctly
        """
        msg_manager = flow.MessageCoder(coder.JsonCoder())
        listener = mock.MagicMock()
        msg_manager.set_listener(listener)
        msg_manager.mqtt_client.handle_received_msg('test', '{ "name": 1, "value": "some_value" }')
        listener.assert_called_with('test', {'name': 1, 'value': 'some_value'})

    @staticmethod
    def test_json_send():
        """ Test that an object is correctly sent and received via a JSON encoder
        """
        client = flow.MqttClient(conf.MQTT_BROKER)
        client.publish = mock.MagicMock()
        msg_manager = flow.MessageCoder(coder.JsonCoder())
        client = msg_manager.mqtt_client
        client.publish = mock.MagicMock()
        msg_manager.publish('test', {'name': 1, 'value': 'some_value'})
        # noinspection PyUnresolvedReferences
        client.publish.assert_called_with('test', '{"name": 1, "value": "some_value"}')


class RouterTests(unittest.TestCase):
    """ Test message routing between the HausNet environment and the external world
    """
    @staticmethod
    async def command_pipeline():
        """ Test pipeline from command input down to the MQTT client
        """
        #router = manager.InterfaceRouter()
        #node = device.NodeDevice('name/AAA000')
        #switch = device.BasicSwitch('test')
        #mqtt_client = manager.HausNetMqttClient()

        source = from_iterable([{'device': 'some_device'}, {'device': 'test_switch1'}, {'device': 'test_switch2'}])
        switch1 = source | op.filter(lambda input: input['device'] == 'test_switch1')
        switch2 = source | op.filter(lambda input: input['device'] == 'test_switch2')
        switch3 = source | op.filter(lambda input: input['device'] == 'test_switch1')

        await subscribe(switch1, AsyncAnonymousObserver(RouterTests.print_value))
        await subscribe(switch2, AsyncAnonymousObserver(RouterTests.print_value))
        await subscribe(switch3, AsyncAnonymousObserver(RouterTests.print_value))

    @staticmethod
    async def print_value(value):
        print(value)


    @staticmethod
    def test_command_pipeline():
        loop = asyncio.get_event_loop()
        loop.run_until_complete(RouterTests.command_pipeline())
        loop.close()
