##
# Classes to build devices of different types, wire them up, and, provide convenient lookups to devices without
# needing to traverse device tree.
#
from abc import ABC, abstractmethod
from typing import Dict, Any, Union, List, Tuple
import importlib
import logging

import hausnet.devices as devices

log = logging.getLogger(__name__)


class BuilderError(Exception):
    pass


class DeviceBuilder(ABC):
    """Builds a specific device from configuration. Each concrete device type should have a corresponding builder.
    """
    @abstractmethod
    def build_from_blueprint(self, device_name: str, blueprint: Dict[str, Any]) -> devices.Device:
        """Given a structured build blueprint, build a device

        :param device_name: The name of the device in firmware.
        :param blueprint:   A dictionary containing the config values in the format above.
        :returns: The built object, with the type the builder is for.
        """
        pass


class BasicSwitchBuilder(DeviceBuilder):
    """Builds a basic switch from a blueprint dictionary. Configuration structure:
            {
              'type': 'basic_switch',
            }
        The name of the basic switch is the name of the firmware device in the node that contains it.
    """
    def build_from_blueprint(self, device_name: str, blueprint: Dict[str, Any]) -> devices.Device:
        """Given a plan dictionary as above, construct the device

            :param device_name: The name of the device in firmware.
            :param blueprint:   A blueprint in the form of the dictionary above.
            :returns: The built BasicSwitch object.
        """
        return devices.BasicSwitch(device_name)


class CompoundDeviceBuilder(ABC):
    """Iteratively builds the constituent devices of a CompoundDevice"""
    def __init__(self, builder_registry: 'DeviceBuilderRegistry'):
        self.registry = builder_registry

    @abstractmethod
    def build_from_blueprints(self, owner: devices.CompoundDevice, blueprints: Dict[str, Any]):
        for device_name, blueprint in blueprints.items():
            if



class NodeDeviceBuilder(CompoundDeviceBuilder):
    """Builds a node device from a blueprint dictionary. Configuration structure:
            {
              'type': 'node',
              'devices':
                    {
                    'device1': {...(device blueprint)...}
                    ...
                    }
            }
        The name of the basic switch is the name of the firmware device in the node that contains it.

        @TODO: Deal with node modules & their configuration vs. shared configuration.
    """
    def build_from_blueprint(self, device_name: str, blueprint: Dict[str, Any]) -> devices.Device:
        """Given a plan dictionary as above, construct the device

            :param device_name: The name of the device in firmware.
            :param blueprint:   A blueprint in the form of the dictionary above.
            :returns: The built BasicSwitch object.
        """
        node = devices.NodeDevice(device_name)
        if 'devices' not in blueprint or len(blueprint['devices']) == 0:
            raise BuilderError(f'No constituent devices for node: {device_name}')
        self.
        return node


class DeviceBuilderRegistry:
    """Maps device types to their builders"""
    def __init__(self):
        """Sets up the device type -> builder mapping"""
        self._registry = {
            'node':         NodeDeviceBuilder(),
            'basic_switch': BasicSwitchBuilder(),
        }

    def get_builder(self, device_class: str):
        """Get the builder for a specific device type

            :param device_class: Name of the class to be built
            :raises BuilderError: When the builder for the specified type cannot be found
        """
        if device_class not in self._registry:
            raise BuilderError(f"Device type not found: {device_class}")
        return self._registry[device_class]


class DeviceTreeBuilder():
    """Class that can build a whole device tree from a specification array. Each element of the array contains
    the definition of a device in the top level of the device hierarchy, typically, NodeDevices. Each NodeDevice
    can have one or more constituent StatefulDevices under its control. An example:
            {
                'sonoff_switch/1ABF00':                  # Name of device (from firmware)
                {
                    'type':         'NodeDevice',        # The name of the class that should be created.
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
            -> Tuple[Dict[str, Device], List[str]]:
        """ Builds a node / device tree from a dictionary-based description.
        """
        self.error_buffer = []
        tree = self.build_tree_node(description)
        return tree, self.error_buffer

    def build_tree_node(self, level_spec: List[Dict[str, Union[int,float,str,List[Dict]]]]) \
            -> Dict[str, Device]:
        """ Recursively build the tree by building out classes depth-first. I.e.:
                - Loop through all class specs given.
                - If a device has subsidiary devices, call this function with the spec for those devices
                - Assemble the result and return it up

            :param  level_spec: The specification for the tree level - a list of all class specs at the current level
            :return Dictionary that contains fully instantiated device objects, e.g. all their subsidiary devices
                    have also been fully instantiated, indexed by device name.
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

    def instantiate_class(self, buildable_class, config_params: Dict[str, Any]) -> Union[Device, None]:
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
            if key in ('name', 'devices') or key in init_params:
                continue
            setattr(device, key, config_param)
        return device

    def add_error(self, error_desc: str):
        """ Append and error description to the error buffer

            TODO: Consider using logging instead (e.g. log output goes to real log and to client)
        """
        self.error_buffer.append(error_desc)