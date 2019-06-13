import unittest as test
import asyncio
import logging
from typing import cast

import janus

from hausnet.flow import AsyncStreamFromQueue, MessageStream, AsyncStreamToQueue
from hausnet.operators.operators import HausNetOperators as Op
from hausnet.devices import BasicSwitch, NodeDevice
from hausnet.coders import JsonCoder

logger = logging.getLogger(__name__)


class UpstreamTests(test.TestCase):
    """Test the upstream data flow"""
    def setUp(self) -> None:
        """Creates an event loop to use in the tests, and re-initializes the UpStream class"""
        self.loop = asyncio.new_event_loop()

    def test_node_subscribe_to_topic_stream(self) -> None:
        """Test that different nodes can subscribe to streams based on their own topics"""
        node_1 = NodeDevice('vendorname_switch/ABC012')
        node_2 = NodeDevice('vendorname_heating/345DEF')
        messages = {'stream_1': [], 'stream_2': []}
        in_queue = janus.Queue(loop=self.loop)
        source = AsyncStreamFromQueue(self.loop, cast(asyncio.Queue, in_queue.async_q))
        # Stream operation: Only forward messages on topics belonging to the node
        stream_1 = (
                source
                | Op.filter(lambda x: x['topic'].startswith(node_1.topic_prefix()))
        )
        stream_2 = (
                source
                | Op.filter(lambda x: x['topic'].startswith(node_2.topic_prefix()))
        )
        up_stream_1 = MessageStream(self.loop, source, stream_1, AsyncStreamToQueue(asyncio.Queue(loop=self.loop)))
        up_stream_2 = MessageStream(self.loop, source, stream_2, AsyncStreamToQueue(asyncio.Queue(loop=self.loop)))
        messages = [
            {'topic': 'hausnet/vendorname_switch/ABC012/upstream', 'message': 'my_message_1'},
            {'topic': 'hausnet/vendorname_switch/ABC012/downstream', 'message': 'my_message_2'},
            {'topic': 'ns2/vendorname_switch/ABC012', 'message': 'my_message_3'},
            {'topic': 'hausnet/vendorname_heating/345DEF', 'message': 'my_message_4'},
            {'topic': 'hausnet/othervendor_switch/BCD678/downstream', 'message': 'my_message_5'}
        ]
        decoded_messages = {}

        async def main():
            for message in messages:
                in_queue.sync_q.put(message)
            while in_queue.async_q.qsize() > 0:
                logger.debug("Upstream in queue size: %s", str(in_queue.async_q.qsize()))
                await asyncio.sleep(0.01)
            for index, up_stream in {1: up_stream_1, 2: up_stream_2}.items():
                decoded_messages[index] = []
                while up_stream.sink.queue.qsize() > 0:
                    decoded_messages[index].append(await up_stream.sink.queue.get())
                    up_stream.sink.queue.task_done()
                up_stream.out_task.cancel()
            source.stream_task.cancel()

        self.loop.run_until_complete(main())
        self.assertEqual(2, len(decoded_messages[1]), "Expected two messages in stream_1")
        self.assertEqual(1, len(decoded_messages[2]), "Expected one message in stream_2")

    def test_node_decodes_json(self):
        """Test that a node can be used to decode JSON"""
        node = NodeDevice('vendorname_switch/ABC012')
        node.coder = JsonCoder()
        in_queue = janus.Queue(loop=self.loop)
        source = AsyncStreamFromQueue(self.loop, cast(asyncio.Queue, in_queue.async_q))
        # Stream operations:
        #   1. Only forward messages on topics belonging to the node
        #   2. Decode the message from JSON into a dictionary
        stream = (
                source
                | Op.filter(lambda x: x['topic'].startswith(node.topic_prefix()))
                | Op.map(lambda x: node.coder.decode(x['message']))
        )
        up_stream = MessageStream(self.loop, source, stream, AsyncStreamToQueue(asyncio.Queue(loop=self.loop)))
        message = {
            'topic':   'hausnet/vendorname_switch/ABC012/upstream',
            'message': '{"switch": {"state": "OFF", "other": ["ON", "OFF"]}}'
        }
        decoded_messages = []

        async def main():
            in_queue.sync_q.put(message)
            decoded_messages.append(await up_stream.sink.queue.get())
            up_stream.sink.queue.task_done()
            up_stream.source.stream_task.cancel()
            up_stream.out_task.cancel()

        self.loop.run_until_complete(main())
        self.assertEqual(1, len(decoded_messages), "Expected one decoded message")
        self.assertEqual(
            {'switch': {'state': 'OFF', 'other': ['ON', 'OFF']}},
            decoded_messages[0],
            "Decoded message structure expected to reflect JSON structure"
            )

    def test_device_gets_message(self):
        """ Test that devices belonging to a node receives messages intended for it
        """
        node = NodeDevice('vendorname_switch/ABC012')
        node.coder = JsonCoder()
        switch_1 = BasicSwitch('switch_1')
        switch_2 = BasicSwitch('switch_2')
        node.devices = {
            'switch_1': switch_1,
            'switch_2': switch_2,
        }
        in_queue = janus.Queue(loop=self.loop)
        source = AsyncStreamFromQueue(self.loop, cast(asyncio.Queue, in_queue.async_q))
        up_streams = []
        for (key, device) in node.devices.items():
            # Stream operations:
            #   1. Only forward messages on topics belonging to the node
            #   2. Decode the message from JSON into a dictionary
            #   3. Only forward messages to the device that are intended (or partly intended) for it
            #   4. Pick out the part of the message intended for the device (each root key represents a device)
            #   5. Tap the stream to store new device state values.
            up_stream = (
                    source
                    | Op.filter(lambda msg: msg['topic'].startswith(node.topic_prefix()))
                    | Op.map(lambda msg: node.coder.decode(msg['message']))
                    | Op.filter(lambda msg_dict, device_id=device.device_id: device_id in msg_dict)
                    | Op.map(lambda msg_dict, device_id=device.device_id: msg_dict[device_id])
                    | Op.tap(lambda dev_msg, dev=device: dev.state.set_value(dev_msg['state']))
            )
            up_streams.append(
                MessageStream(self.loop, source, up_stream, AsyncStreamToQueue(asyncio.Queue(loop=self.loop)))
            )
        messages = [
            {
                'topic':   'hausnet/vendorname_switch/ABC012/upstream',
                'message': '{"switch_1": {"state": "OFF"}}'
            },
            {
                'topic':   'hausnet/vendorname_switch/ABC012/upstream',
                'message': '{"switch_2": {"state": "ON"}}'
            },
            {
                'topic':   'hausnet/vendorname_switch/ABC012/upstream',
                'message': '{"switch_1": {"state": "UNDEFINED"}}'
            },
        ]
        decoded_messages = []

        async def main():
            for message in messages:
                in_queue.sync_q.put(message)
            while in_queue.async_q.qsize() > 0:
                logger.debug("Upstream in-queue size: %s", str(in_queue.async_q.qsize()))
                await asyncio.sleep(0.01)
            for stream in up_streams:
                while stream.sink.queue.qsize() > 0:
                    decoded_messages.append(await stream.sink.queue.get())
                    stream.sink.queue.task_done()
                stream.out_task.cancel()
            source.stream_task.cancel()

        self.loop.run_until_complete(main())
        self.assertEqual(3, len(decoded_messages), "Expected device messages")
        self.assertIn({'state': 'OFF'}, decoded_messages, "'OFF' state should be present")
        self.assertIn({'state': 'ON'}, decoded_messages, "'ON' state should be present")
        self.assertIn({'state': 'UNDEFINED'}, decoded_messages, "'UNDEFINED' state should be present'")
        self.assertEqual('UNDEFINED', switch_1.state.value, "switch_1 state should be 'UNDEFINED'")
        self.assertEqual('ON', switch_2.state.value, "switch_2 state should be 'ON'")
