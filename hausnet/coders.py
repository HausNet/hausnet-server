import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any

logger = logging.getLogger(__name__)


class Coder(ABC):
    """ Base class for all en/decoders
    """
    @classmethod
    @abstractmethod
    def decode(cls, encoded_value: str) -> Dict[str, Any]:
        pass

    @classmethod
    @abstractmethod
    def encode(cls, decoded_value: Dict[str, Any]) -> str:
        pass


class JsonCoder(Coder):
    """ JSON decoder
    """
    @classmethod
    def decode(cls, json_str: str) -> Dict[str, Any]:
        """ Turns a JSON string into a dict

        :param json_str: A JSON string to be decoded into a dictionary
        :returns: A dictionary object representing the JSON
        :except: Passes on JSON decoding exceptions - have to be handled at a higher level.
        """
        logger.debug("Decoding: %s", json_str)
        try:
            return json.loads(json_str)
        except (ValueError, KeyError, TypeError) as e:
            logger.error("JSON decoding failed for: %s", )
            raise e

    @classmethod
    def encode(cls, json_obj: Any) -> str:
        """ Encode a dictionary object as a JSON string.

        :param json_obj: Object containing real data structure.
        :return: The data structure rendered as JSON
        """
        logger.debug("Decoding: %s", json_obj)
        try:
            return json.dumps(json_obj, separators=(',', ':'))
        except (ValueError, TypeError) as e:
            logger.error("JSON encoding failed for: %s", json_obj)
            raise e
