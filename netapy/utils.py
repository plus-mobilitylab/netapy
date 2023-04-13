import operator
import re

def clean_string(obj, keep = "[^a-zA-Z0-9_.:\-]", strip = True):
  substr = re.sub(keep, "", obj)
  if strip:
    return substr.strip()
  else:
    return substr

def split_string(obj):
  char = re.sub("[^a-zA-Z]", "", obj)
  if char == "":
    char = None
  num = string_to_numeric(re.sub("[^0-9.\-]", "", obj), fail = False)
  return char, num

def string_to_operator(obj, fail = True):
  mapping = {
    "l": operator.lt,
    "lt": operator.lt,
    "<": operator.lt,
    "le": operator.le,
    "leq": operator.le,
    "<=": operator.le,
    "e": operator.eq,
    "eq": operator.eq,
    "==": operator.eq,
    "ne": operator.ne,
    "neq": operator.ne,
    "!=": operator.ne,
    "ge": operator.ge,
    "geq": operator.ge,
    ">=": operator.ge,
    "g": operator.gt,
    "gt": operator.gt,
    ">": operator.gt
  }
  try:
    return mapping[obj]
  except KeyError:
    if fail:
      raise ValueError(f"Could not convert string to operator: {obj}")
    return None

def string_to_numeric(obj, fail = True):
  try:
    return int(obj)
  except ValueError:
    try:
      return float(obj)
    except ValueError:
      if fail:
        raise ValueError(f"Could not convert string to numeric: {obj}")
      return None

def string_to_boolean(obj, fail = True):
  if obj in ["True", "true", "1", "1.0"]:
    return True
  if obj in ["False", "false", "0", "0.0"]:
    return False
  if fail:
    raise ValueError(f"Could not convert string to boolean: {obj}")
  return None