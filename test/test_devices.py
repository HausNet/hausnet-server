import unittest as test

from hausnet.devices import *


class DeviceTests(test.TestCase):
    """ Tests for the device infrastructure
    """
    def test_basic_switch_core(self):
        """ Test the core operations on a basic switch.
        """
        switch = BasicSwitch("switch")
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
