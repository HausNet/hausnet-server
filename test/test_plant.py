import asynctest as atest
import unittest.mock as mock

from hausnet.builders import PlantBuilder
from hausnet.states import OnOffState


class PlantTest(atest.TestCase):
    """ Test plant operation. """

    @atest.strict()
    @mock.patch('hausnet.plant.MqttClient')
    async def test_send_state_change_to_switch(self, mock_client):
        """ Send a state change downstream. """
        blueprint = {
            'test_node': {
                'type':      'node',
                'device_id': 'test/ABC123',
                'devices':   {
                    'test_switch': {
                        'type':      'basic_switch',
                        'device_id': 'switch',
                    },
                    'test_sensor': {
                        'type':      'sensor',
                        'device_id': 'thermo',
                        'config': {
                            'state.type': 'float',
                            'state.unit': 'F'
                        }
                    }
                }
            },
            'test_node_2': {
                'type':      'node',
                'device_id': 'test/123ABC',
                'devices':   {
                    'test_switch': {
                        'type':      'basic_switch',
                        'device_id': 'switch',
                    },
                    'test_sensor': {
                        'type':      'sensor',
                        'device_id': 'thermo',
                        'config': {
                            'state.type': 'float',
                            'state.unit': 'F'
                        }
                    }
                }
            }
        }
        plant = PlantBuilder().build(blueprint, self.loop)
        assembly = plant.device_assemblies['test_node.test_switch']
        await assembly.client_in_queue.put({'state': OnOffState.ON})
        await assembly.client_in_queue.put({'state': OnOffState.OFF})
        plant.start()
        await assembly.client_in_queue.join()
        plant.stop()
        queue = plant.downstream_dest_queue
        self.assertEqual(
            {'topic': 'hausnet/test/ABC123/downstream', 'message': '{"switch":{"state":"ON"}}'},
            queue.sync_q.get_nowait(),
            "ON message to switch expected"
        )
        self.assertEqual(
            {'topic': 'hausnet/test/ABC123/downstream', 'message': '{"switch":{"state":"OFF"}}'},
            queue.sync_q.get_nowait(),
            "OFF message to switch expected"
        )

    @atest.strict()
    @mock.patch('hausnet.plant.MqttClient')
    async def test_receive_sensor_sample(self, mock_client):
        blueprint = {
            'test_node': {
                'type':      'node',
                'device_id': 'test/ABC123',
                'devices':   {
                    'test_sensor': {
                        'type':      'sensor',
                        'device_id': 'thermo',
                        'config': {
                            'state.type': 'float',
                            'state.unit': 'F'
                        }
                    }
                }
            },
            'test_node_2': {
                'type':      'node',
                'device_id': 'test/123ABC',
                'devices':   {
                    'test_switch': {
                        'type':      'basic_switch',
                        'device_id': 'switch',
                    },
                    'test_sensor': {
                        'type':      'sensor',
                        'device_id': 'thermo',
                        'config': {
                            'state.type': 'float',
                            'state.unit': 'F'
                        }
                    }
                }
            }
        }
        plant = PlantBuilder().build(blueprint, self.loop)
        await plant.upstream_source.queue.put({
            'topic': 'hausnet/test/ABC123/upstream',
            'message': '{"thermo":{"state":13}}'
        })
        await plant.upstream_source.queue.put({
            'topic': 'hausnet/test/ABC123/upstream',
            'message': '{"thermo":{"state":14}}'
        })
        plant.start()
        await plant.upstream_source.queue.join()
        plant.stop()
        assembly = plant.device_assemblies['test_node.test_sensor']
        queue = assembly.upstream_pipe.sink.queue
        self.assertEqual(
            {'state': 13},
            queue.get_nowait(),
            "Value of 13 expected from thermo device"
        )
        self.assertEqual(
            {'state': 14},
            queue.get_nowait(),
            "Value of 14 expected from thermo device"
        )
