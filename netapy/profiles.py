import copy
import yaml

from abc import abstractmethod
from collections import OrderedDict
from collections.abc import MutableMapping

from netapy import utils
from netapy.exceptions import NetapyProfileError


class Profile(dict):

  def __init__(self, obj = None, name = None, validate = False, parse = False):
    super(Profile, self).__init__({} if obj is None else obj)
    self.name = name
    self._parsed = None
    if validate:
      self.validate()
    if parse:
      self.parse()

  def __setitem__(self, key, value):
    self._parsed = None
    super(Profile, self).__setitem__(key, value)

  def __delitem__(self, key):
    self._parsed = None
    super(Profile, self).__delitem__(key)

  def clear(self):
    self._parsed = None
    super(Profile, self).clear()

  def pop(self, key, default):
    if key in self.keys():
      self._parsed = None
    super(Profile, self).pop(key, default)

  def popitem(self):
    self._parsed = None
    super(Profile, self).popitem()

  def setdefault(self, key, default):
    if key in self.keys():
      self._parsed = None
    super(Profile, self).setdefault(key, default)

  def update(self, other):
    self._parsed = None
    super(Profile, self).update(other)

  @property
  def name(self):
    return self._name

  @name.setter
  def name(self, value):
    assert isinstance(value, str) or value is None
    self._name = value

  @property
  def parsed(self):
    if self._parsed is None:
      self.parse()
    return self._parsed

  @abstractmethod
  def validate(self):
    pass

  @abstractmethod
  def parse(self):
    self._parse = dict(self) # Placeholder


class NetascoreProfile(Profile):

  def __init__(self, obj = None, name = None, validate = False, parse = False):
    super(NetascoreProfile, self).__init__(obj, name, validate, parse)

  @classmethod
  def from_file(cls, file):
    with open(file, "r") as f:
      obj = yaml.safe_load(f)
    return cls(obj)

  def validate(self):
    # TODO: Create workflow to validate profile.
    pass

  def parse(self):
    out = dict(self)
    # Parse weights.
    out["weights"] = {k:v for k, v in out["weights"].items() if v is not None}
    # TODO: Parse overrides.
    # Parse indicator mappings.
    raw = out["indicator_mapping"]
    parsed = {i["indicator"]:self.parse_indicator_mapping(i) for i in raw}
    out["indicator_mapping"] = parsed
    self._parsed = out

  @staticmethod
  def parse_indicator_mapping(obj):
    raw = copy.deepcopy(obj)
    # Parse name.
    name = raw.pop("indicator")
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
    if isinstance(obj, float) or isinstance(obj, int) or isinstance(obj, bool):
      members = [obj]
      has_null = False
    else:
      obj = str(obj)
      if obj.startswith("{") and obj.endswith("}"):
        members = [utils.clean_string(x) for x in obj[1:-1].split(",")]
      else:
        members = [utils.clean_string(obj)]
      members_upper = [x.upper() for x in members]
      if any([x in members_upper for x in ["NONE", "NAN", "NULL"]]):
        has_null = True
      else:
        has_null = False
      try:
        members = [utils.string_to_numeric(x) for x in members]
      except ValueError:
        try:
          members = [utils.string_to_boolean(x) for x in members]
        except ValueError:
          pass
    if has_null:
      return lambda x: x in members or pd.isnull(x)
    else:
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
    if isinstance(obj, dict):
      return NetascoreProfile.parse_indicator_mapping(obj)
    raise NetapyProfileError(f"Unsupported assignment value: {obj}")