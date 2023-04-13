import copy
import yaml

from abc import abstractmethod
from collections import OrderedDict

from netapy import utils
from netapy.exceptions import NetapyProfileError


class Profile(dict):

  def __init__(self, obj = None, name = None, validate = False):
    dict_obj = {} if obj is None else obj
    super(Profile, self).__init__(dict_obj)
    self.name = name
    self._obj = dict_obj
    self._is_valid = None
    if validate:
      self.validate()

  @property
  def name(self):
    return self._name

  @name.setter
  def name(self, value):
    assert isinstance(value, str)
    self._name = value

  @property
  def is_valid(self):
    if self._is_valid is None:
      self.validate()
    return self.is_valid

  @abstractmethod
  def validate(self):
    self._is_valid = True # Placeholder

  @abstractmethod
  def parse(self):
    pass


class NetascoreProfile(Profile):

  def __init__(self, obj = None, name = None, validate = False):
    super(NetascoreProfile, self).__init__(obj, name, validate)

  @classmethod
  def from_file(cls, file):
    with open(file, "r") as f:
      obj = yaml.safe_load(f)
    return cls(obj)

  def validate(self):
    # TODO: Create workflow to validate profile.
    self._is_valid = True

  def parse(self):
    out = copy.deepcopy(self._obj)
    # Parse weights.
    out["weights"] = {k:v for k, v in out["weights"].items() if v is not None}
    # TODO: Parse overrides.
    # Parse indicator mappings.
    raw = out["indicator_mapping"]
    parsed = {i["indicator"]:self.parse_indicator_mapping(i) for i in raw}
    out["indicator_mapping"] = parsed
    return out

  @staticmethod
  def parse_indicator_mapping(obj):
    raw = copy.deepcopy(obj)
    # Parse name.
    name = raw.pop("indicator")
    # Validate.
    if len(raw) != 1:
      if len(raw) > 1:
        raise NetapyProfileError(
          f"Multiple mappings are defined for indicator '{name}'"
        )
      else:
        raise NetapyProfileError(
          f"No mapping is defined for indicator '{name}'"
        )
    # Parse type.
    maptype = list(raw.keys())[0]
    raw = raw[maptype]
    # Parse rules.
    try:
      default = raw.pop("_default_")
    except KeyError:
      default = float("nan")
    # Parse mapping object.
    if maptype == "mapping":
      key_parser = NetascoreProfile.parse_set_membership
    elif maptype == "classes":
      key_parser = NetascoreProfile.parse_condition
    else:
      raise NetapyProfileError(
        f"Unsupported mapping type for indicator '{name}': {maptype}"
      )
    value_parser = NetascoreProfile.parse_assignment
    parsed = OrderedDict({key_parser(k):value_parser(v) for k, v in raw.items()})
    return {"type": maptype, "indicator": name, "rules": parsed, "default": default}

  @staticmethod
  def parse_set_membership(obj):
    # TODO: Parse keys that are None or nan or "NULL".
    if isinstance(obj, float) or isinstance(obj, int) or isinstance(obj, bool):
      members = [obj]
    else:
      obj = str(obj)
      if obj.startswith("{") and obj.endswith("}"):
        members = [utils.clean_string(x) for x in obj[1:-1].split(",")]
      else:
        members = [utils.clean_string(obj)]
      try:
        members = [utils.string_to_numeric(x) for x in members]
      except ValueError:
        try:
          members = [utils.string_to_boolean(x) for x in members]
        except ValueError:
          pass
    return lambda x: x in members

  @staticmethod
  def parse_condition(obj):
    obj = utils.clean_string(str(obj))
    operator_str, operand_str = utils.split_string(obj)
    if not operator_str:
      operator_str = "e" # Default
    operator = utils.string_to_operator(operator_str)
    operand = utils.string_to_numeric(operand_str)
    return lambda x: operator(x, operand)

  @staticmethod
  def parse_assignment(obj):
    if obj is None:
      return float("nan")
    if isinstance(obj, float) or isinstance(obj, int) or isinstance(obj, bool):
      return obj
    if isinstance(obj, str):
      return utils.clean_string(obj)
    # TODO: Parse assignments that are nested indicator mappings.
    raise NetapyProfileError(f"Unsupported assignment value: {obj}")