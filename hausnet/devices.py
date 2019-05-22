##
# Classes that represent embedded devices of various types. The architecture reflects the physical structure of a
# HausNet protocol plant. A very brief overview:
#   1. Networked nodes (aka HausNodes) serve as the gateway to hardware devices connected to the node, with no
#      network access themselves. Nodes are modeled as "NodeDevice" devices, and are associated with unique MQTT topics.
#   2. Node devices themselves have no state - only StatefulDevices do. Either of them can have configuration though.
#   3. Basic sensors and actuators are considered as being part of the network node that controls them. They are
#      modeled by function-specific classes (e.g. "BasicSwitch", "BinarySensor", etc.). "CompoundDevice" encapsulates
#      the ability of one device to contain other devices. Right now, only NodeDevices are compound, but the option
#      to have compound devices of other types are kept open.
#   4. All the devices in the network can be directly addressed when changing and querying state. The framework
#      transparently mediates message flow via the nodes to their eventual destination. The destination (e.g. the
#      switch on an ESP8266 board) is determined by: a) The device_id of the node containing the device;
#      b) The device_id of the device on the node board. E.g. one may have a "sonoff_switch/A7E4BB" node, which
#      has a "basic_switch" defined. But from a high-level perspective, one would address the "basic_switch" by a
#      contextual label, e.g. "bathroom_lights", directly, without concern about the actual network structure or
#      how the messages flow.
#

from abc import ABC
from typing import Dict, List

from hausnet.coders import JsonCoder
from hausnet.flow import MessageCoder, BufferedAsyncSource
from hausnet.states import *


class Device(ABC):
    """The base class for all devices.

     :param device_id: The ID of the device in the device firmware. This is unique locally, i.e. on a node with
                       multiple switch sub-devices, these might be named "switch_1", "switch_2", etc.
     """
    def __init__(self, device_id: str):
        self.device_id: str = device_id


class SubDevice(Device, ABC):
    """A device that belongs to another device, typically to gain network access through the owner (but other
    functions may be added, e.g. treating devices as a group.
    """
    def __init__(self, device_id: str, owner_device: 'CompoundDevice' = None):
        """Sets the owner of this device

        :param device_id:    For the Device initializer.
        :param owner_device: The device that owns this one. E.g. a switch would have the node it is physically a part
                             of as its owner.
        """
        super().__init__(device_id)
        self.owner_device = owner_device


class CompoundDevice(Device, ABC):
    """Device that contains a collection of devices managed by itself."""
    def __init__(self, device_id: str, devices: List[SubDevice] = None):
        """Creates sub-devices if any are provided"""
        super().__init__(device_id)
        self.sub_devices: Dict[str, Device] = {}
        if devices:
            self.add_sub_devices(devices)

    def add_sub_device(self, device: SubDevice) -> None:
        """Add a new sub-device, accessible from the compound device via its device_id, and set its owner relationship
        back to this device object.

        :param device: A sub-device that belongs to this device.
        """
        device.owner_device = self
        self.sub_devices[device.device_id] = device

    def add_sub_devices(self, devices: List[SubDevice]) -> None:
        """ Add multilple devices to the object

            :param devices: List of SubDevice objects, each with a device_id.
        """
        for device in devices:
            self.add_sub_device(device)


class StatefulDevice(SubDevice, ABC):
    """ Device with a state. Stateful devices never have direct network access, and needs to be part of a
    CompoundDevice in order to be available for control / measurement.
    """
    def __init__(self, device_id: str, state: State, owner_device: 'CompoundDevice' = None):
        """Set the owner of this device, so it is reachable from here

        :param device_id:    The on-board device ID
        :param state:        The state of the device
        :param owner_device: The compound device that owns this device
        """
        super().__init__(device_id, owner_device)
        self.state = state

    @property
    def state(self):
        return self.__state

    @state.setter
    def state(self, new_state: State):
        self.__state = new_state


class MeasuringDevice(ABC):
    """ Allows a device to measure internal or external values, or, react to events. Note: This assumes the class this
        is used in is derived from AtomicDevice, and thus has a state variable and a container.
    """
    def receive_state(self, new_value: Any):
        """ Called by the container whenever a state update is received
        """
        # noinspection PyUnresolvedReferences
        self.state.value = new_value


class ControlDevice(ABC):
    """ Allows a device to control hardware. Records requests for state changes without disturbing the current state,
        in the expectation that the current state will be updated after some condition is met (e.g. the device
        confirms the new state). Turns the state change request into a message, then places it into a central
        (class-wide) message buffer for further processing and eventual delivery to the device.
    """
    control_buffer: BufferedAsyncSource = None

    def __init__(self):
        # noinspection PyTypeChecker
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
    pass


class BasicSwitch(StatefulDevice, ControlDevice):
    """ A basic switch that can control an output"""
    def __init__(self, device_id: str, owner_device: CompoundDevice = None):
        super().__init__(device_id, OnOffState(), owner_device)


class NodeDevice(CompoundDevice):
    """ Encapsulates a network node (a "HausNode"), providing network access to one or more sensors or actuators.

        The node device_id is used both as a way to identify the node, but also, as part of topics  subscribed to, or
        published to for the node itself, and any devices the node is a gateway for.

        The node device_id follows the format "vendor_device/mac_lsb", with vendor the device_id of the vendor, e.g.
        "sonoff", and device a vendor-specific device device_id (e.g. "basic" for the SonOff Basic Switch), and a
        device-specific ID consisting of the last six hexadecimal digits of the device MAC. This device_id is provided
        by the node itself during discovery.

        All topics are prefaced with 'hausnet/' to namespace the HausNet environment separately from other users of
        the MQTT broker. Each node has one downstream and one upstream topic. E.g. for a node device_id of
        "sonoff_basic/ABC123", these are the topics:
                hausnet/sonoff_basic/ABC123/downstream
                hausnet/sonoff_basic/ABC123/upstream
    """
    # The namespace prefix for all topics
    TOPIC_NAMESPACE = 'hausnet/'

    def __init__(self, name: str, devices: List[SubDevice] = None, coder: MessageCoder = JsonCoder()):
        """ Constructor. By default, uses Json de/coding

            :param name: The node device_id (see class doc)
        """
        super().__init__(name, devices)
        self.name = name
        self.coder = coder

    def owns_topic(self, packet: Dict[str, str]) -> bool:
        """ Given a message packet, consisting of a dictionary with the 'topic' key's entry the full topic device_id,
            decide whether the topic is "owned" by this node.
        """
        return packet['topic'].startswith(self.topic_prefix())

    def topic_prefix(self):
        """ Return the prefix to any topic owned by this node
        """
        return self.TOPIC_NAMESPACE + self.name

    def add_devices(self, devices: List[CompoundDevice]):
        """ Add devices to the node. Indexes the devices by their names, and sets the reference back to the node on
            each device.

            :param devices: List of AtomicDevice objects, each with a device_id.
        """
        self.add_devices(devices)
        for device in devices:
            device.node = self
