import unittest as test
import asyncio

from aioreactive.core import subscribe, AsyncAnonymousObserver, AsyncObservable
# noinspection PyPep8Naming
from aioreactive.core import Operators as op
import hausnet.flow as flow
from hausnet.device import *
from hausnet.coder import JsonCoder


class DeviceTests(test.TestCase):
    """ Tests for the device infrastructure
    """
    def setUp(self):
        self.msg_manager = flow.MessageCoder(JsonCoder())

    def test_basic_switch_core(self):
        """ Test the core operations on a basic switch.
        """
        switch = BasicSwitch()
        self.assertListEqual(
            switch.state.possible_values,
            [OnOffState.UNDEFINED, OnOffState.OFF, OnOffState.ON],
            "Switch should be able to accept UNDEFINED/OFF/ON as values"
        )
        self.assertEqual(switch.state.value, OnOffState.UNDEFINED, "Initial value should be UNDEFINED")
        switch.state.value = OnOffState.OFF
        self.assertEqual(switch.state.value, OnOffState.OFF, "Switch should be turned OFF")
        with self.assertRaises(ValueError):
            switch.state.value = 'INVALID'
        switch.state.value = OnOffState.ON
        self.assertEqual(switch.state.value, OnOffState.ON, "Switch should be turned ON")

    def test_node_subscribe_to_topic_stream(self):
        """ Test that different nodes can subscribe to streams based on their own topics
        """
        node_1 = NodeDevice('vendorname_switch/ABC012')
        node_2 = NodeDevice('vendorname_heating/345DEF')
        messages = {'stream_1': [], 'stream_2': []}

        async def stream_1_observer(message: Dict[str, str]):
            print('stream_1:')
            print(message)
            messages['stream_1'].append(message)

        async def stream_2_observer(message: Dict[str, str]):
            print('stream_2:')
            print(message)
            messages['stream_2'].append(message)

        async def main(source: AsyncObservable):
            stream_1 = (
                    source
                    | op.filter(lambda x: x['topic'].startswith(node_1.topic_prefix()))
                )
            stream_2 = (
                    source
                    | op.filter(lambda x: x['topic'].startswith(node_2.topic_prefix()))
                )
            await subscribe(stream_1, AsyncAnonymousObserver(stream_1_observer))
            await subscribe(stream_2, AsyncAnonymousObserver(stream_2_observer))

        client = flow.MqttClient()
        client.upstreamQueue.test_message_limit = 5
        in_messages = [
            {'topic': 'hausnet/vendorname_switch/ABC012/upstream', 'message': 'my_message_1'},
            {'topic': 'hausnet/vendorname_switch/ABC012/downstream', 'message': 'my_message_2'},
            {'topic': 'ns2/vendorname_switch/ABC012', 'message': 'my_message_3'},
            {'topic': 'hausnet/vendorname_heating/345DEF', 'message': 'my_message_4'},
            {'topic': 'hausnet/othervendor_switch/BCD678/downstream', 'message': 'my_message_5'}
            ]
        for in_message in in_messages:
            client.upstreamQueue.queue.put_nowait(in_message)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(AsyncObservable.from_async_iterable(client.upstreamQueue))) #client.upstreamSource
        loop.close()
        self.assertEqual(2, len(messages['stream_1']), "Expected two messages in stream_1")
        self.assertEqual(1, len(messages['stream_2']), "Expected one message in stream_2")
