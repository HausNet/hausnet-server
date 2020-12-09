import os
from typing import cast

import unittest
import asynctest
import janus
import aioreactive.core as aioreact

from hausnet.devices import NodeDevice, BasicSwitch, OnOffState
from hausnet.flow import *
from hausnet.operators.operators import HausNetOperators as Op
from hausnet.coders import JsonCoder

logger = logging.getLogger(__name__)


class FlowTests(asynctest.TestCase):
    """ Test basic flow from source to sink. """

    @asynctest.strict()
    def test_stream_flows_from_queue(self) -> None:
        """ Test that an observer can be connected to an async queue. """
        async def stream_from_queue() -> asynctest.CoroutineMock:
            """ Stream a list of integers from a mock queue, and return the a CoroutineMock holding the results. """
            in_q = asynctest.Mock(asyncio.Queue)
            q_input = [0, 1, 2]
            in_q.get.side_effect = q_input
            asend = asynctest.CoroutineMock()
            src = AsyncStreamFromFixedQueue(in_q, len(q_input))
            src.start()
            await aioreact.subscribe(src, aioreact.AsyncAnonymousObserver(asend))
            return asend

        result: asynctest.CoroutineMock = self.loop.run_until_complete(stream_from_queue())
        self.assertEqual(3, len(result.await_args_list))
        result.assert_any_await(0)
        result.assert_any_await(1)
        result.assert_any_await(2)

    @asynctest.strict()
    def test_stream_flows_into_queue(self) -> None:
        """ Test data flowing in to a sink queue from a source. """
        async def stream_to_queue() -> asynctest.Mock:
            """ Stream a list of integers into a mock queue, and return the mock queue. """
            out_q = asynctest.create_autospec(asyncio.Queue)
            sink = AsyncStreamToQueue(out_q)
            await sink.asend(0)
            await sink.asend(1)
            await sink.asend(2)
            return out_q

        result: asynctest.Mock = self.loop.run_until_complete(stream_to_queue())
        self.assertEqual(3, len(result.method_calls))
        result.put.assert_any_await(0)
        result.put.assert_any_await(1)
        result.put.assert_any_await(2)

    @asynctest.strict()
    def test_src_streams_to_dest(self) -> None:
        """ Test that data can be streamed from a mock queue to a mock queue. """
        async def stream_src_to_dest() -> asynctest.CoroutineMock:
            """ Stream a list of integers via a source queue to a sink queue. Returns the mock output queue for
                inspection.
            """
            in_q = asynctest.Mock(asyncio.Queue)
            q_input = [0, 1, 2]
            in_q.get.side_effect = q_input
            out_q = asynctest.Mock(asyncio.Queue)
            src = AsyncStreamFromFixedQueue(in_q, len(q_input))
            sink = AsyncStreamToQueue(out_q)
            src.start()
            await aioreact.subscribe(src, sink)
            return out_q

        result: asynctest.Mock = self.loop.run_until_complete(stream_src_to_dest())
        self.assertEqual(3, result.put.await_count)
        result.put.assert_any_await(0)
        result.put.assert_any_await(1)
        result.put.assert_any_await(2)

    @asynctest.strict()
    def test_flow_through_pipe_with_ops(self) -> None:
        """ Test data flowing from downstream queue to upstream, with operations performed. """
        async def stream_through_pipe() -> janus.Queue:
            """ Stream queue values through a mapping operation to an output queue. """
            in_q = janus.Queue()
            in_q.sync_q.put_nowait(1)
            in_q.sync_q.put_nowait(2)
            out_q = janus.Queue()
            sink = AsyncStreamToQueue(cast(asyncio.Queue, out_q.async_q))
            src = AsyncStreamFromQueue(cast(asyncio.Queue, in_q.async_q))
            stream = (
                    src
                    | Op.map(lambda msg: 10*msg)
                    | Op.map(lambda msg: 3*msg)
            )
            pipe = MessagePipe(src, stream, sink)
            pipe.source.start()
            await in_q.async_q.join()
            pipe.source.stop()
            return out_q

        result_q: janus.Queue = self.loop.run_until_complete(stream_through_pipe())
        self.assertEqual(2, result_q.sync_q.qsize())
        self.assertEqual(30, result_q.sync_q.get())
        self.assertEqual(60, result_q.sync_q.get())


class DownstreamTests(asynctest.TestCase):
    """Test sending command and configuration data downstream"""

    @asynctest.strict()
    def test_downstream_state_change_propagates(self):
        """ Test that state changes entering downstream causes changes in devices. """

        async def stream_state_up(switch, state):
            """ Async execution part of test. """
            in_queue = janus.Queue()
            source = AsyncStreamFromQueue(cast(asyncio.Queue, in_queue.async_q))
            out_queue = janus.Queue()
            sink = AsyncStreamToQueue(cast(asyncio.Queue, out_queue.async_q))
            topic = topic_name(switch.get_node().topic_prefix(), TOPIC_DIRECTION_DOWNSTREAM)
            stream = (
                    source
                    | Op.map(lambda msg, sw=switch: (
                        {sw.device_id: msg},
                        logger.debug("Map device ID %s: %s", sw.device_id, msg)
                    )[0])
                    | Op.map(lambda msg, sw=switch: sw.get_node().coder.encode(msg))
                    | Op.map(lambda msg, sw=switch, tp=topic: {'topic': tp, 'message': msg})
            )
            # noinspection PyUnusedLocal
            message_stream = MessagePipe(source, stream, sink)
            source.start()
            in_queue.sync_q.put(state)
            await in_queue.async_q.join()
            source.stop()
            return out_queue

        device = BasicSwitch('switch_a')
        NodeDevice('node/ABC123', {'switch_1': device, })
        result_q: janus.Queue = self.loop.run_until_complete(stream_state_up(device, {'state': OnOffState.ON}))
        self.assertEqual(
            {'topic': 'hausnet/node/ABC123/downstream', 'message': '{"switch_a":{"state":"ON"}}'},
            result_q.sync_q.get(),
            "switch_1 should have 'ON' message"
        )

    @asynctest.strict()
    def test_upstream_change_propagates(self):
        """ Test that device state changes end up in the downstream buffer. """

        async def stream_state_down(switches):
            """ Executes the async behavior to be verified. """
            out_queue = janus.Queue()
            sink = AsyncStreamToQueue(cast(asyncio.Queue, out_queue.async_q))
            # Holds tuples of Janus in-queues & pipes
            down_pipes = []
            for switch in switches:
                topic = topic_name(switch.get_node().topic_prefix(), TOPIC_DIRECTION_DOWNSTREAM)
                logger.debug("Topic: %s", topic)
                in_queue = janus.Queue()
                source = AsyncStreamFromQueue(cast(asyncio.Queue, in_queue.async_q))
                stream = (
                        source
                        | Op.map(lambda msg, sw=switch: {sw.device_id: msg})
                        | Op.map(lambda msg, sw=switch: sw.get_node().coder.encode(msg))
                        | Op.map(lambda msg, sw=switch, tp=topic: {'topic': tp, 'message': msg})
                )
                pipe = MessagePipe(source, stream, sink)
                source.start()
                down_pipes.append((in_queue, pipe))
            down_pipes[0][0].sync_q.put({'state': OnOffState.ON})
            down_pipes[1][0].sync_q.put({'state': OnOffState.ON})
            down_pipes[2][0].sync_q.put({'state': OnOffState.ON})
            down_pipes[0][0].sync_q.put({'state': OnOffState.OFF})
            await asyncio.gather(
                down_pipes[0][1].source.queue.join(),
                down_pipes[1][1].source.queue.join(),
                down_pipes[2][1].source.queue.join()
            )
            for pipe in down_pipes:
                pipe[1].source.stop()
            return out_queue

        devices = [BasicSwitch('switch_a'), BasicSwitch('switch_b'), BasicSwitch('switch_c')]
        NodeDevice('node/ABC123', {'switch_1': devices[0], 'switch_2': devices[1]})
        NodeDevice('node/456DEF', {'switch_3': devices[2]})
        result_q = self.loop.run_until_complete(stream_state_down(devices))
        messages = []
        while not result_q.sync_q.empty():
            messages.append(result_q.sync_q.get())
        self.assertEqual(4, len(messages), "Expected four messages to be generated by devices")
        self.assertIn(
            {'topic': 'hausnet/node/ABC123/downstream', 'message': '{"switch_a":{"state":"ON"}}'},
            messages,
            "switch_1 should have 'ON' message"
        )
        self.assertIn(
            {'topic': 'hausnet/node/ABC123/downstream', 'message': '{"switch_a":{"state":"OFF"}}'},
            messages,
            "switch_1 should have 'OFF' message"
        )
        self.assertIn(
            {'topic': 'hausnet/node/ABC123/downstream', 'message': '{"switch_b":{"state":"ON"}}'},
            messages,
            "switch_2 should have 'ON' message"
        )
        self.assertIn(
            {'topic': 'hausnet/node/456DEF/downstream', 'message': '{"switch_c":{"state":"ON"}}'},
            messages,
            "switch_3 should have 'ON' message"
        )


class UpstreamTests(asynctest.TestCase):
    """ Test the upstream data flow. """

    @asynctest.strict()
    def test_node_subscribe_to_topic_stream(self) -> None:
        """ Test that different nodes can subscribe to streams based on their own topics. """
        test_nodes = [
            NodeDevice('vendorname_switch/ABC012'),
            NodeDevice('vendorname_heating/345DEF'),
        ]
        test_messages = [
            {'topic': 'hausnet/vendorname_switch/ABC012/upstream', 'message': 'my_message_1'},
            {'topic': 'hausnet/vendorname_switch/ABC012/downstream', 'message': 'my_message_2'},
            {'topic': 'ns2/vendorname_switch/ABC012', 'message': 'my_message_3'},
            {'topic': 'hausnet/vendorname_heating/345DEF', 'message': 'my_message_4'},
            {'topic': 'hausnet/othervendor_switch/BCD678/downstream', 'message': 'my_message_5'}
        ]

        async def subscribe_to_stream(nodes, messages):
            """ Pipe messages from one common source to two sinks based on which node their meant for. """
            in_queue = janus.Queue()
            source = AsyncStreamFromFixedQueue(cast(asyncio.Queue, in_queue.async_q), len(messages))
            # Stream operation: Only forward messages on topics belonging to the node
            stream_1 = (
                    source
                    | Op.filter(lambda x: x['topic'].startswith(nodes[0].topic_prefix()))
            )
            stream_2 = (
                    source
                    | Op.filter(lambda x: x['topic'].startswith(nodes[1].topic_prefix()))
            )
            up_stream_1 = MessagePipe(source, stream_1, AsyncStreamToQueue(asyncio.Queue()))
            up_stream_2 = MessagePipe(source, stream_2, AsyncStreamToQueue(asyncio.Queue()))
            for message in messages:
                in_queue.sync_q.put(message)
            source.start()
            await source.queue.join()
            decoded_messages = {}
            for index, up_stream in {1: up_stream_1, 2: up_stream_2}.items():
                decoded_messages[index] = []
                while up_stream.sink.queue.qsize() > 0:
                    decoded_messages[index].append(await up_stream.sink.queue.get())
                    up_stream.sink.queue.task_done()
            return decoded_messages

        result_messages = self.loop.run_until_complete(subscribe_to_stream(test_nodes, test_messages))
        self.assertEqual(2, len(result_messages[1]), "Expected two messages in stream_1")
        self.assertEqual(1, len(result_messages[2]), "Expected one message in stream_2")

    @asynctest.strict
    def test_node_decodes_json(self):
        """Test that a node can be used to decode JSON"""
        node = NodeDevice('vendorname_switch/ABC012')
        node.coder = JsonCoder()
        message = {
            'topic':   'hausnet/vendorname_switch/ABC012/upstream',
            'message': '{"switch": {"state": "OFF", "other": ["ON", "OFF"]}}'
        }

        async def decode_json(msg):
            in_queue = janus.Queue()
            source = AsyncStreamFromQueue(cast(asyncio.Queue, in_queue.async_q))
            # Stream operations:
            #   1. Only forward messages on topics belonging to the node
            #   2. Decode the message from JSON into a dictionary
            stream = (
                    source
                    | Op.filter(lambda x: x['topic'].startswith(node.topic_prefix()))
                    | Op.map(lambda x: node.coder.decode(x['message']))
            )
            up_stream = MessagePipe(source, stream, AsyncStreamToQueue(asyncio.Queue()))
            in_queue.sync_q.put(msg)
            source.start()
            decoded = await up_stream.sink.queue.get()
            up_stream.sink.queue.task_done()
            await source.queue.join()
            source.stop()
            return decoded

        decoded_msg = self.loop.run_until_complete(decode_json(message))
        self.assertEqual(
            {'switch': {'state': 'OFF', 'other': ['ON', 'OFF']}},
            decoded_msg,
            "Decoded message structure expected to reflect JSON structure"
            )

    def test_device_gets_message(self):
        """ Test that devices belonging to a node receives messages intended for it . """
        test_node = NodeDevice('vendorname_switch/ABC012')
        test_node.coder = JsonCoder()
        switch_1 = BasicSwitch('switch_1')
        switch_2 = BasicSwitch('switch_2')
        test_node.sub_devices = {
            'switch_1': switch_1,
            'switch_2': switch_2,
        }

        async def send_message(node: NodeDevice):
            in_queue = janus.Queue()
            source = AsyncStreamFromQueue(cast(asyncio.Queue, in_queue.async_q))
            up_streams = []
            for (key, device) in node.sub_devices.items():
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
                    MessagePipe(source, up_stream, AsyncStreamToQueue(asyncio.Queue()))
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
            for message in messages:
                in_queue.sync_q.put(message)
            source.start()
            await source.queue.join()
            source.stop()
            messages = []
            for stream in up_streams:
                while stream.sink.queue.qsize() > 0:
                    messages.append(await stream.sink.queue.get())
                    stream.sink.queue.task_done()
            return messages

        decoded_messages = self.loop.run_until_complete(send_message(test_node))
        self.assertEqual(3, len(decoded_messages), "Expected three device messages")
        self.assertIn({'state': 'OFF'}, decoded_messages, "'OFF' state should be present")
        self.assertIn({'state': 'ON'}, decoded_messages, "'ON' state should be present")
        self.assertIn({'state': 'UNDEFINED'}, decoded_messages, "'UNDEFINED' state should be present'")
        self.assertEqual('UNDEFINED', switch_1.state.value, "switch_1 state should be 'UNDEFINED'")
        self.assertEqual('ON', switch_2.state.value, "switch_2 state should be 'ON'")


@unittest.skipIf("MQTT_TEST" not in os.environ.keys(), "MQTT test not enabled")
class MqttClientTests(unittest.TestCase):
    """ Test the MQTT client. """

    def test_message_receipt(self):
        """ Test that messages sent are received. """
        sub_queue = queue.Queue()
        pub_queue = queue.Queue()
        MqttClient(pub_queue, sub_queue)
        pub_queue.put({'topic': 'hausnet/test/ABC123/upstream', 'message': 'hello'})
        message = sub_queue.get(block=True, timeout=30)
        self.assertEqual(
            {'topic': 'hausnet/test/ABC123/upstream', 'message': b'hello'},
            message,
            "Expected same message that was sent"
        )
