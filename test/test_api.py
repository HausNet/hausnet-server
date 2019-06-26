import unittest
import asyncio

from hausnet.builders import DevicePlantBuilder, DeviceInterface
from hausnet.states import OnOffState
from test.helpers import AsyncTest


class ApiTest(AsyncTest):
    """Test from the viewpoint of a client (e.g. HASS)"""

    def test_send_state_to_switch(self):
        interfaces = DevicePlantBuilder(self.loop).build({
            'test_node': {
                'type': 'node',
                'device_id': 'test/ABC123',
                'devices': {
                    'test_switch': {
                        'type': 'basic_switch',
                        'device_id': 'switch',
                    }
                }
            }
        })
        interface = interfaces['test_node.test_switch']

        async def main():
            await interface.in_queue.put({'state': OnOffState.ON})
            await interface.in_queue.put({'state': OnOffState.OFF})
            await interface.in_queue.join()

        self.loop.run_until_complete(main())
        queue = DeviceInterface.downstream_dest_queue
        self.assertEqual(
            {'topic': 'hausnet/test/ABC123/downstream', 'message': '{"switch":{"state":"ON"}}'},
            queue.sync_q.get(),
            "ON message to switch expected"
        )
        self.assertEqual(
            {'topic': 'hausnet/test/ABC123/downstream', 'message': '{"switch":{"state":"OFF"}}'},
            queue.sync_q.get(),
            "OFF message to switch expected"
        )
