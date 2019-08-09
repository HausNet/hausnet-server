from typing import Any, Union


class State:
    """Encapsulates an atomic state. The state value type is open-ended, to allow for different sensors and actuator
    values, e.g. 'on'/'off', integer values, floating point values, strings, etc. Allows for overriding the getters
    and setters in derived classes to customize behavior.
    """
    possible_values = None

    def __init__(self, value: Any):
        self.value = value

    @property
    def value(self) -> Any:
        return self._value

    @value.setter
    def value(self, new_value: Any):
        if not self.in_possible_values(new_value):
            raise ValueError("%s not a valid value for $s", new_value, self.__class__.__name__)
        self._value = new_value

    def set_value(self, new_value: Any):
        """Alias of the setter for use in lambdas"""
        self.value = new_value

    @classmethod
    def in_possible_values(cls, value):
        raise NotImplementedError("Bare State does not yet have possible values")


class DiscreteState(State):
    """Encapsulates a State that can only take one of a small number of values"""
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


class ContinuousState(State):
    """A class that has states on a continuum, instead of named states."""

    def __init__(self, value: Union[int, float] = 0, unit: str = None):
        super().__init__(value)
        self._unit = unit

    @classmethod
    def in_possible_values(cls, value) -> bool:
        return type(value) in (float, int)

    @property
    def unit(self) -> Union[str, None]:
        """Return the unit of the state values, if one exists."""
        return self._unit if self._unit else None

    @unit.setter
    def unit(self, value: str):
        """Set the unit the state values are measured in."""
        self._unit = value


class FloatState(ContinuousState):
    """A state that is a floating-point number."""

    @classmethod
    def in_possible_values(cls, value) -> bool:
        return type(value) in (int, float)

    def __init__(self, unit: str = None):
        super().__init__(0, unit)
        if unit:
            self._unit = unit
