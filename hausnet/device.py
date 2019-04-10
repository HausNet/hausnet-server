from abc import ABC, abstractmethod
from typing import Any, Dict, List
from aioreactive.core.observables import T

from hausnet.coder import JsonCoder
from hausnet.flow import TappedStream, MessageCoder, BufferedAsyncSource


class State:
    """ Encapsulates an atomic state. The state value type is open-ended, to allow for different sensors and actuator
        values, e.g. 'on'/'off', integer values, floating point values, strings, etc. Allows for overriding the getters
        and setters in derived classes to customize behavior.
    """
    possible_values = None

    def __init__(self, value: Any):
        self.value = value

    @property
    def value(self):
        return self.__value

    @value.setter
    def value(self, new_value: Any):
        if not self.in_possible_values(new_value):
            raise ValueError("%s not a valid value for $s", new_value, self.__class__.__name__)
        self.__value = new_value

    def set_value(self, new_value: Any):
        """ Alias of the setter for use in lambdas
        """
        self.value = new_value

    @classmethod
    def in_possible_values(cls, value):
        raise NotImplementedError("Bare State does not yet have possible values")


class DiscreteState(State):
    """ Encapsulates a State that can only take one of a small number of values
    """
    possible_values = []

    @classmethod
    def in_possible_values(cls, value: Any) -> bool:
        return value in cls.possible_values


class OnOffState(DiscreteState):
    """ An On/Off state that can be used for a Switch, for instance.
    """
    UNDEFINED = 'UNDEFINED'
    OFF = 'OFF'
    ON = 'ON'
    possible_values = [UNDEFINED, OFF, ON]

    def __init__(self, value: str = UNDEFINED):
        super().__init__(value)


class VirtualDevice(ABC):
    """ A representation of a device that cannot be decomposed into smaller representations. E.g. a simple switch,
        a thermometer, etc. It has a name, a state, and an owner node.
    """
    def __init__(self, name: str, state: State, node: 'NodeDevice' = None):
        """ Initialize a device.

            :param name:  The name of the device, has to be unique in the NodeDevice namespace.
            :param state: The state of the device, initialized to some default value.
            :param node:  The node this device belongs to. Usually set by the node when the device is added to it.
        """
        self.state = state
        self.name = name
        self.node = node

        @property
        def state(self):
            return self.__state

        @state.setter
        def state(self, new_state: State):
            self.__state = new_state


class MeasuringMixin(ABC):
    """ Allows a device to measure internal or external values, or, react to events. Note: This assumes the class this
        is used in is derived from AtomicDevice, and thus has a state variable and a container.
    """
    def receive_state(self, new_value: Any):
        """ Called by the container whenever a state update is received
        """
        self.state.value = new_value


class ControllingMixin(ABC):
    """ Allows a device to control hardware. Records requests for state changes without disturbing the current state,
        in the expectation that the current state will be updated after some condition is met (e.g. the device
        confirms the new state). Turns the state change request into a message, then places it into a central
        (class-wide) message buffer for further processing and eventual delivery to the device.
    """
    control_buffer: BufferedAsyncSource = None

    def __init__(self):
        self.future_state: State = None

    def new_state(self, new_state: State):
        """ Records the new state, and transmits it to the device by means of the async downstream buffer. Expects
            the discrepancy between the current state and the requested state to be reconciled later, outside of
            this class' context.
        """
        self.future_state = new_state
        self.control_buffer.buffer({'device': self, 'state': self.future_state.value})


class DeviceManagementInterface(ABC):
    """ Interface to a device from a client's (of the library) perspective. I.e. ways to control the device and receive
        data from it
    """


class BasicSwitch(VirtualDevice, ControllingMixin):
    """ A basic switch that can control an output, and can report back on its own internal state (at the device)
    """
    def __init__(self, name: str = 'basic_switch', node: 'NodeDevice' = None):
        super().__init__(name, OnOffState(), node)


class NodeDevice():
    """ Encapsulates a network node (a "HausNode"), providing network access to one or more sensors or actuators.

        The node name is used both as a way to identify the node, but also, as part of topics  subscribed to, or
        published to for the node itself, and any devices the node is a gateway for.

        The node name follows the format "vendor_device/mac_lsb", with vendor the name of the vendor, e.g. "sonoff",
        and device a vendor-specific device name (e.g. "basic" for the SonOff Basic Switch), and a device-specific
        ID consisting of the last six hexadecimal digits of the device MAC. This name is provided by the node itself
        during discovery.

        All topics are prefaced with 'hausnet/' to namespace the HausNet environment separately from other users of
        the MQTT broker. Each node has one downstream and one upstream topic. E.g. for a node name of
        "sonoff_basic/ABC123", these are the topics:
                hausnet/sonoff_basic/ABC123/downstream
                hausnet/sonoff_basic/ABC123/upstream

        TODO: Currently node will happily "own" upstream topics used for downstream-heading data? Block this?
    """

    # The namespace prefix for all topics
    TOPIC_NAMESPACE = 'hausnet/'

    def __init__(self, name: str, devices: List[VirtualDevice] = []):
        """ Constructor. By default, uses Json de/coding

            :param name: The node name (see class doc)
        """
        self.name = name
        self.devices = {}
        self.add_devices(devices)

    def owns_topic(self, packet: Dict[str, str]) -> bool:
        """ Given a message packet, consisting of a dictionary with the 'topic' key's entry the full topic name,
            decide whether the topic is "owned" by this node.
        """
        return packet['topic'].startswith(self.topic_prefix())

    def topic_prefix(self):
        """ Return the prefix to any topic owned by this node
        """
        return self.TOPIC_NAMESPACE + self.name

    def add_devices(self, devices: List[VirtualDevice]):
        """ Add devices to the node. Indexes the devices by their names, and sets the reference back to the node on
            each device.

            :param devices: List of AtomicDevice objects, each with a name.
        """
        for device in devices:
            device.node = self
            self.devices[device.name] = device
