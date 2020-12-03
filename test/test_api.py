import asynctest

from hausnet.builders import PlantBuilder
from hausnet.states import OnOffState


class ApiTest(asynctest.TestCase):
    """Test from the viewpoint of a client (e.g. HASS)"""

    @asynctest.strict()
    async def test_send_state_to_switch(self):
        blueprint = {
            'test_node': {
                'type':      'node',
                'device_id': 'test/ABC123',
                'devices':   {
                    'test_switch': {
                        'type':      'basic_switch',
                        'device_id': 'switch',
                    }
                }
            }
        }
        plant = PlantBuilder().build(blueprint, self.loop)
        assembly = plant.device_assemblies['test_node.test_switch']
        plant.upstream_src_queue.sync_q.put({'state': OnOffState.ON})
        plant.upstream_src_queue.sync_q.put({'state': OnOffState.OFF})
        plant.upstream_source.start()
        await plant.upstream_source.queue.join()
        plant.upstream_source.stop()
        queue = plant.downstream_dest_queue
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
