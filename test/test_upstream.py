import unittest as test
from typing import Dict, List
import asyncio
import logging

from aioreactive.core import subscribe, AsyncAnonymousObserver, AsyncStream
from aioreactive.operators import from_iterable

from hausnet.flow import FixedSyncToAsyncBufferedStream, DeviceInterfaceStreams, UpStream
from hausnet.operators.operators import HausNetOperators as Op
from hausnet.devices import BasicSwitch, NodeDevice
from hausnet.coders import JsonCoder

logger = logging.getLogger(__name__)

class UpstreamTests(test.TestCase):
    """Test the upstream data flow"""
    @staticmethod
    def inject_messages(source: FixedSyncToAsyncBufferedStream, messages: List[Dict[str, str]]):
        source.max_messages = len(messages)
        for message in messages:
            source.queue.put(message)

    def test_node_subscribe_to_topic_stream(self):
        """Test that different nodes can subscribe to streams based on their own topics"""
        node_1 = NodeDevice('vendorname_switch/ABC012')
        node_2 = NodeDevice('vendorname_heating/345DEF')
        messages = {'stream_1': [], 'stream_2': []}
        loop = asyncio.new_event_loop()
        source = FixedSyncToAsyncBufferedStream(loop)
        self.inject_messages(
            source,
            [
                {'topic': 'hausnet/vendorname_switch/ABC012/upstream', 'message': 'my_message_1'},
                {'topic': 'hausnet/vendorname_switch/ABC012/downstream', 'message': 'my_message_2'},
                {'topic': 'ns2/vendorname_switch/ABC012', 'message': 'my_message_3'},
                {'topic': 'hausnet/vendorname_heating/345DEF', 'message': 'my_message_4'},
                {'topic': 'hausnet/othervendor_switch/BCD678/downstream', 'message': 'my_message_5'}
            ]
        )

        async def stream_1_observer(message: Dict[str, str]):
            messages['stream_1'].append(message)

        async def stream_2_observer(message: Dict[str, str]):
            messages['stream_2'].append(message)

        async def main():
            # Stream operation: Only forward messages on topics belonging to the node
            stream_1 = (
                source
                | Op.filter(lambda x: x['topic'].startswith(node_1.topic_prefix()))
            )
            # Stream operation: Only forward messages on topics belonging to the node
            stream_2 = (
                source
                | Op.filter(lambda x: x['topic'].startswith(node_2.topic_prefix()))
            )
            await subscribe(stream_2, AsyncAnonymousObserver(stream_2_observer))
            await subscribe(stream_1, AsyncAnonymousObserver(stream_1_observer))
            await source.stream()
        loop.run_until_complete(main())
        loop.close()
        self.assertEqual(2, len(messages['stream_1']), "Expected two messages in stream_1")
        self.assertEqual(1, len(messages['stream_2']), "Expected one message in stream_2")

    def test_node_decodes_json(self):
        """Test that a node can be used to decode JSON"""
        node = NodeDevice('vendorname_switch/ABC012')
        node.coder = JsonCoder()
        # Stream operations:
        #   1. Only forward messages on topics belonging to the node
        #   2. Decode the message from JSON into a dictionary
        stream = (
                UpStream.source_stream
                | Op.filter(lambda x: x['topic'].startswith(node.topic_prefix()))
                | Op.map(lambda x: node.coder.decode(x['message']))
        )
        loop = asyncio.new_event_loop()
        up_stream = UpStream(loop, stream)
        message = {
            'topic':   'hausnet/vendorname_switch/ABC012/upstream',
            'message': '{"switch": {"state": "OFF", "other": ["ON", "OFF"]}}'
        }
        decoded_messages = []

        async def main():
            UpStream.in_queue.sync_q.put(message)
            decoded_messages.append(await up_stream.out_queue.get())
            up_stream.out_queue.task_done()
            up_stream.in_task.cancel()
            up_stream.out_task.cancel()

        loop.run_until_complete(main())
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
        loop = asyncio.new_event_loop()
        up_streams = []
        for (key, device) in node.devices.items():
            # Stream operations:
            #   1. Only forward messages on topics belonging to the node
            #   2. Decode the message from JSON into a dictionary
            #   3. Only forward messages to the device that are intended (or partly intended) for it
            #   4. Pick out the part of the message intended for the device (each root key represents a device)
            #   5. Tap the stream to store new device state values.
            up_stream = (
                    UpStream.source_stream
                    | Op.filter(lambda msg: msg['topic'].startswith(node.topic_prefix()))
                    | Op.map(lambda msg: node.coder.decode(msg['message']))
                    | Op.filter(lambda msg_dict, device_id=device.device_id: device_id in msg_dict)
                    | Op.map(lambda msg_dict, device_id=device.device_id: msg_dict[device_id])
                    | Op.tap(lambda dev_msg, dev=device: dev.state.set_value(dev_msg['state']))
            )
            up_streams.append(UpStream(loop, up_stream))
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
                UpStream.in_queue.sync_q.put(message)
            while UpStream.in_queue.async_q.qsize() > 0:
                logger.debug("UpStream in queue size: %s", str(UpStream.in_queue.async_q.qsize()))
                await asyncio.sleep(0.01)
            for stream in up_streams:
                while stream.out_queue.qsize() > 0:
                    decoded_messages.append(await stream.out_queue.get())
                    stream.out_queue.task_done()
            UpStream.in_task.cancel()
            for stream in up_streams:
                stream.out_task.cancel()

        loop.run_until_complete(main())
        self.assertEqual(3, len(decoded_messages), "Expected device messages")
        self.assertEqual({'state': 'OFF'}, decoded_messages[0], "switch_1 state should be 'OFF'")
        self.assertEqual({'state': 'ON'}, decoded_messages[1], "switch_2 state should be 'ON'")
        self.assertEqual({'state': 'UNDEFINED'}, decoded_messages[2], "switch_1 state should be 'UNDEFINED'")
        self.assertEqual('UNDEFINED', switch_1.state.value, "switch_1 state should be 'UNDEFINED'")
        self.assertEqual('ON', switch_2.state.value, "switch_2 state should be 'ON'")
