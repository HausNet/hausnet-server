import unittest
import asyncio
from typing import cast
import logging

import janus

from hausnet.devices import NodeDevice, BasicSwitch, OnOffState
from hausnet.flow import topic_name, TOPIC_DIRECTION_DOWNSTREAM, AsyncStreamToQueue, AsyncStreamFromQueue, MessageStream
from hausnet.operators.operators import Operators as Op

logger = logging.getLogger(__name__)


class DownstreamTests(unittest.TestCase):
    """Test sending command and configuration data downstream"""
    def setUp(self) -> None:
        """Creates an event loop to use in the tests, and re-initializes the UpStream class"""
        self.loop = asyncio.new_event_loop()

    def test_set_device_values(self):
        """Test that device state changes end up in the downstream buffer"""
        switches = [BasicSwitch('switch_a'), BasicSwitch('switch_b'), BasicSwitch('switch_c')]
        NodeDevice('node/ABC123', {'switch_1': switches[0], 'switch_2': switches[1]})
        NodeDevice('node/456DEF', {'switch_3': switches[2]})
        out_queue = janus.Queue(loop=self.loop)
        sink = AsyncStreamToQueue(cast(asyncio.Queue, out_queue.async_q))
        down_streams = []
        for switch in switches:
            topic = topic_name(switch.get_node().topic_prefix(), TOPIC_DIRECTION_DOWNSTREAM)
            logger.debug("Topic: %s", topic)
            source = AsyncStreamFromQueue(self.loop, asyncio.Queue(loop=self.loop))
            stream = (
                source
                | Op.map(lambda msg, sw=switch: {sw.device_id: msg})
                | Op.map(lambda msg, sw=switch: sw.get_node().coder.encode(msg))
                | Op.map(lambda msg, sw=switch, tp=topic: {'topic': tp, 'message': msg})
            )
            down_streams.append(MessageStream(self.loop, source, stream, sink))

        async def main():
            await down_streams[0].source.queue.put({'state': OnOffState.ON})
            await down_streams[1].source.queue.put({'state': OnOffState.ON})
            await down_streams[2].source.queue.put({'state': OnOffState.ON})
            await down_streams[0].source.queue.put({'state': OnOffState.OFF})
            while out_queue.sync_q.qsize() < 4:
                await asyncio.sleep(0.01)
            for down_stream in down_streams:
                down_stream.out_task.cancel()
                down_stream.source.stream_task.cancel()

        self.loop.run_until_complete(main())
        messages = []
        while not out_queue.sync_q.empty():
            messages.append(out_queue.sync_q.get())
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
