##
# Classes to build devices of different types, wire them up, and, provide convenient lookups to devices without
# needing to traverse device tree.
#
# TODO: Figure out if blueprint errors should be logged / should break process (it's breaking now)
#
from abc import ABC, abstractmethod
from typing import Dict, Any, Union, List, Tuple, cast
import importlib
import logging

import hausnet.devices as devices

log = logging.getLogger(__name__)


class BuilderError(Exception):
    """Wraps errors encountered during building for convenience"""
    pass


class DeviceBuilder(ABC):
    """Builds a specific device from configuration. Each concrete device type should have a corresponding builder."""

    @abstractmethod
    def from_blueprint(self, device_name: str, blueprint: Dict[str, Any]) -> (devices.SubDevice, devices.NodeDevice):
        """Given a structured build blueprint, build a device.

        :param device_name: The device_id of the device in firmware.
        :param blueprint:   A dictionary containing the config values in the format above.
        :returns: A device, of the type the builder builds.
        """
        pass


class BasicSwitchBuilder(DeviceBuilder):
    """Builds a basic switch from a blueprint dictionary. Configuration structure:
            {
              'type': 'basic_switch',
            }
        The device_id of the basic switch is the device_id of the firmware device in the node that contains it.
    """
    def from_blueprint(self, device_name: str, blueprint: Dict[str, Any]) -> devices.Device:
        """Given a plan dictionary as above, construct the device

            :param device_name: The device_id of the device in firmware.
            :param blueprint:   A blueprint in the form of the dictionary above.
            :returns: The built BasicSwitch object.
        """
        return devices.BasicSwitch(device_name)


class NodeDeviceBuilder(DeviceBuilder):
    """Builds a node device from a blueprint dictionary. Configuration structure:
            {
              'type': 'node',
              'devices':
                    {
                    'device1': {...(device blueprint)...}
                    ...
                    }
            }
        The device_id of the basic switch is the device_id of the firmware device in the node that contains it.

        Building the constituent devices is left to the routine that built the node device.
    """
    def from_blueprint(self, device_name: str, blueprint: Dict[str, Any]) -> devices.Device:
        """Given a plan dictionary as above, construct the device

            :param device_name: The device_id of the device in firmware.
            :param blueprint:   A blueprint in the form of the dictionary above.
            :returns: The built BasicSwitch object.
        """
        return devices.NodeDevice(device_name)


class StructureBuilder:
    """Builds a tree of devices from a blueprint (dictionary). The tree closely mirrors the input blueprint in
    structure, with the difference that the tree holds instantiated & configured devices, while the blueprint
    just holds a text representation of devices and their configuration.
    """
    @classmethod
    def build(cls, blueprint: Dict[str, Any]) -> Dict[str, Union[devices.CompoundDevice, devices.SubDevice]]:
        """Steps through the blueprint components and build a device for each. If a device is a compound device,
        _build_constituents() is called to build each of the sub-devices
        """
        device_tree: Dict[str, Union[devices.Device, devices.CompoundDevice]] = {}
        for key, device_blueprint in blueprint.items():
            builder = DeviceBuilderRegistry.builder_for(device_blueprint['type'])
            device_tree[key] = builder.from_blueprint(key, device_blueprint)
            if not issubclass(device_tree[key].__class__, devices.CompoundDevice):
                continue
            cls._build_sub_devices(device_tree[key], device_blueprint['devices'])
        return device_tree

    @classmethod
    def _build_sub_devices(cls, device: devices.CompoundDevice, blueprints: Dict[str, Dict[str, Any]]):
        for name, blueprint in blueprints.items():
            builder = DeviceBuilderRegistry.builder_for(blueprint['type'])
            device.add_sub_device(builder.from_blueprint(name, blueprint))


class DeviceTreeBuilder:
    """Class that can build a whole device tree from a specification array. Each element of the array contains
    the definition of a device in the top level of the device hierarchy, typically, NodeDevices. Each NodeDevice
    can have one or more constituent StatefulDevices under its control. An example:
            {
                'sonoff_switch/1ABF00':                  # Name of device (from firmware)
                {
                    'type':         'NodeDevice',        # The device_id of the class that should be created.
                    ...                                  # TODO: Configuration values TBD - use established patterns
                    'devices':
                    [
                        'switch':
                        {
                            'type': 'BasicSwitch',
                        }
                    ]
                },
                {
                     ...                                        # Definition of next device at same level in tree
                },
                ...
    """
    def __init__(self):
        """ Initializes the error buffer
        """
        self.error_buffer = []

    def build(self, description: List[Dict[str, Union[int,float,str,List[Dict]]]]) \
            -> Tuple[Dict[str, devices.Device], List[str]]:
        """ Builds a node / device tree from a dictionary-based description.
        """
        self.error_buffer = []
        tree = self.build_tree_node(description)
        return tree, self.error_buffer

    def build_tree_node(self, level_spec: List[Dict[str, Union[int,float,str,List[Dict]]]]) \
            -> Dict[str, devices.Device]:
        """ Recursively build the tree by building out classes depth-first. I.e.:
                - Loop through all class specs given.
                - If a device has subsidiary devices, call this function with the spec for those devices
                - Assemble the result and return it up

            :param  level_spec: The specification for the tree level - a list of all class specs at the current level
            :return Dictionary that contains fully instantiated device objects, e.g. all their subsidiary devices
                    have also been fully instantiated, indexed by device device_id.
        """
        tree_node = {}
        for class_spec in level_spec:
            device = None
            try:
                module = importlib.import_module('hausnet.device')
                buildable_class = getattr(module, class_spec['type'])
                device = self.instantiate_class(buildable_class, class_spec)
                if not device:
                    self.add_error(f"Device {class_spec['type']} not found.")
                    continue
                if 'devices' in class_spec:
                    device.devices = self.build_tree_node(class_spec['devices'])
            except KeyError as e:
                self.add_error("Device type missing for: %s" % class_spec)
            except AttributeError as e:
                self.add_error("Non-existent device type for: %s" % class_spec)
            except BuilderError as e:
                self.add_error(" for: %s" % class_spec)
            tree_node[device.name] = device
        return tree_node

    def instantiate_class(self, buildable_class, config_params: Dict[str, Any]) -> Union[devices.Device, None]:
        """ Given a buildable class object, verify all the required config params are present, then create an
            object with the given config params. Assign any left-over config params as simple member variables.
        """
        init_params = {}
        # Extract the required parameters, and create the object with them
        for param_var in buildable_class.required_params:
            if param_var not in config_params:
                self.add_error("Missing required parameter '%s' for: %s" % (param_var, config_params))
                return None
            init_params[param_var] = config_params[param_var]
        device = buildable_class(**init_params)
        # Set the optional attributes
        for key, config_param in config_params.items():
            if key in ('device_id', 'devices') or key in init_params:
                continue
            setattr(device, key, config_param)
        return device

    def add_error(self, error_desc: str):
        """ Append and error description to the error buffer

            TODO: Consider using logging instead (e.g. log output goes to real log and to client)
        """
        self.error_buffer.append(error_desc)


class DeviceBuilderRegistry:
    """Maps device type handles to their builders"""

    # The device type handle -> builder mapping
    _registry: Dict[str, 'DeviceBuilder'] = {
        'node':         NodeDeviceBuilder(),
        'basic_switch': BasicSwitchBuilder()
    }

    @classmethod
    def builder_for(cls, type_handle: str) -> DeviceBuilder:
        """Get the builder for a specific device type handle.

            :param type_handle: Handle to the class of the appropriate builder object.
            :raises BuilderError: When the builder for the specified type cannot be found.
        """
        if type_handle not in cls._registry:
            raise BuilderError(f"Device type handle not found: {type_handle}")
        return cls._registry[type_handle]
