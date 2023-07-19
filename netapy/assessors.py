import networkx as nx
import inspect
import logging
import random

from abc import abstractmethod

from netapy import defaults
from netapy.profiles import NetascoreProfile
from netapy.exceptions import NetapyNetworkError

logger = logging.getLogger(__name__)


class Assessor():

  def __init__(self):
    pass

  @abstractmethod
  def run(self, network):
    pass


class NetascoreAssessor(Assessor):

  def __init__(self, profile, naming_config = None):
    self.profile = profile
    if naming_config is None:
      self.naming_config = defaults.NETASCORE_NAMING_CONFIG
    else:
      self.naming_config = naming_config
    self._subindex_cache = {}
    self._attribute_cache = {}

  @property
  def profile(self):
    return self._profile

  @profile.setter
  def profile(self, value):
    if isinstance(value, str):
      try:
        self._profile = defaults.NETASCORE_PROFILES[value]
      except KeyError:
        raise ValueError(f"Unknown profile: '{value}'")
    elif isinstance(value, NetascoreProfile):
      value.validate()
      self._profile = value
    else:
      raise ValueError(f"Unsupported profile type: '{type(value)}'")

  @property
  def naming_config(self):
    return self._naming_config

  @naming_config.setter
  def naming_config(self, value):
    self._naming_config = value

  def run(self, network, **config):
    return self.generate_index(network, **config)

  def clean(self, network, **config):
    # TODO: Create workflow to remove all netascore columns from network.
    raise NotImplementedError()

  def generate_index(self, network, digits = 2, read = False, write = True,
                     read_subs = False, write_subs = True, read_attrs = False,
                     write_attrs = True, return_data = False):
    metadata = self._init_metadata(kind = "index", directed = True)
    # Read values from the network if read = True and the index exists.
    if read:
      try:
        data = self._read_from_network(metadata, network)
      except NetapyNetworkError:
        pass
      else:
        if return_data:
          metadata.update(data)
        return metadata
    # Otherwise derive the indices by taking a weighted average of subindices.
    config = {
      "read": read_subs,
      "write": write_subs,
      "read_attrs": read_attrs,
      "write_attrs": write_attrs,
      "return_data": True
    }
    self._subindex_cache = self.generate_subindices(network, **config)
    edges = network.edges
    data = {"data": {}}
    indexer = lambda e, d: self._index_edge(e, d, digits)
    for direction in ["forward", "backward"]:
      data["data"][direction] = {e:indexer(e, direction) for e in edges}
    # Post-process.
    if write:
      self._write_to_network(data, metadata, network)
    if return_data:
      metadata.update(data)
    self._subindex_cache.clear()
    return metadata

  def generate_subindices(self, network, read = False, write = True,
                          read_attrs = False, write_attrs = True, return_data = False):
    out = {}
    config = {
      "read": read,
      "write": write,
      "read_attrs": read_attrs,
      "write_attrs": write_attrs,
      "return_data": return_data,
      "clear_cache": False
    }
    self._attribute_cache.clear()
    for i in self.profile.parsed["weights"]:
      out[i] = self.generate_subindex(i, network, **config)
    self._attribute_cache.clear()
    return out

  def generate_subindex(self, label, network, read = False, write = True,
                        read_attrs = False, write_attrs = True,
                        return_data = False, clear_cache = True):
    directed = self._is_directed_attribute(label)
    metadata = self._init_metadata(label, kind = "index", directed = directed)
    # Read values from the network if read = True and the index exists.
    if read:
      try:
        data = self._read_from_network(metadata, network)
      except NetapyNetworkError:
        pass
      else:
        if return_data:
          metadata.update(data)
        return metadata
    # Otherwise derive the indices by mapping its corresponding attribute values.
    # Fetch the values of the attribute belonging to the subindex.
    config = {"read": read_attrs, "write": write_attrs, "return_data": True}
    try:
      attr = self._attribute_cache[label]
    except KeyError:
      attr = self.generate_attribute(label, network, **config)
      self._attribute_cache[label] = attr
    # Fetch the mapping that maps the attribute values to the index values.
    mapping = self.profile.parsed["indicator_mapping"][label]
    # It may be that the mapping also references other attributes.
    # In that case we need to fetch those attribute values as well.
    other_labels = []
    def _find_attrs(obj):
      for assignment in obj["rules"].values():
        if isinstance(assignment, dict):
          other_labels.append(assignment["indicator"])
          _find_attrs(assignment)
    _find_attrs(mapping)
    for x in other_labels:
      if x not in self._attribute_cache:
        other_attr = self.generate_attribute(x, network, **config)
        self._attribute_cache[x] = other_attr
    # Generate the subindex values for each edge.
    indexer = lambda e, d: self._subindex_edge(e, label, mapping, d)
    if directed:
      data = {"data": {}}
      for direction in ["forward", "backward"]:
        idxs = {e:indexer(e, direction) for e in attr["data"][direction]}
        data["data"][direction] = idxs
    else:
      data = {}
      idxs = {e:indexer(e, None) for e in attr["data"]}
      data["data"] = idxs
    # Post-process.
    if write:
      self._write_to_network(data, metadata, network)
    if return_data:
      metadata.update(data)
    if clear_cache:
      self._attribute_cache.clear()
    return metadata

  def generate_attribute(self, label, network, read = False, write = True,
                         return_data = False):
    directed = self._is_directed_attribute(label)
    metadata = self._init_metadata(label, kind = "attribute", directed = directed)
    # Read values from the network if read = True and the attribute exists.
    if read:
      try:
        data = self._read_from_network(metadata, network)
      except NetapyNetworkError:
        pass
      else:
        if return_data:
          metadata.update(data)
        return metadata
    # Otherwise derive the attribute values from the network data.
    deriver = getattr(self, f"derive_{label}")
    if metadata["directed"]:
      data = {"data": {}}
      for direction in ["forward", "backward"]:
        data["data"][direction] = deriver(network, direction)
    else:
      data = {}
      data["data"] = deriver(network)
    # Post-process.
    if write:
      self._write_to_network(data, metadata, network)
    if return_data:
      metadata.update(data)
    return metadata

  def derive_access_car(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = [True, True, True, True, False]
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_access_bicycle(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = [True, True, True, True, False]
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_access_pedestrian(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = [True, True, True, True, False]
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_bridge(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    vals = [False] * len(keys)
    return {k:v for k, v in zip(keys, vals)}

  def derive_tunnel(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    vals = [False] * len(keys)
    return {k:v for k, v in zip(keys, vals)}

  def derive_bicycle_infrastructure(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = ["bicycle_way", "mixed_way", "bicycle_lane", "bus_lane", "no"]
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_pedestrian_infrastructure(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = ["pedestrian_area", "pedestrian_way", "mixed_way", "stairs", "sidewalk", "no"]
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_gradient(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = [-4, -3, -2, -1, 0, 1, 2, 3, 4]
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_max_speed(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = range(0, 130)
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_max_speed_greatest(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = range(0, 130)
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_road_category(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = ["primary", "secondary", "residential", "service", "calmed", "no_mit", "path"]
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_designated_route(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = ["local", "regional", "national", "international", "unknown", "no"]
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_number_lanes(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = range(0, 10)
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_width(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = range(0, 10)
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_pavement(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = ["asphalt", "gravel", "cobble", "soft"]
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_parking(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = ["yes", "no"]
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_crossings(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = range(0, 10)
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_buildings(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = range(0, 100)
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_facilities(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = range(0, 10)
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_greenness(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = range(0, 100)
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_water(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = [True, False]
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def derive_noise(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    keys = network.edges
    pool = range(0, 100)
    vals = random.choices(pool, k = len(keys))
    return {k:v for k, v in zip(keys, vals)}

  def _init_metadata(self, label = None, kind = "attribute", directed = False):
    if directed:
      obj = {"type": kind, "directed": True, "name": {}}
      for direction in ["forward", "backward"]:
        name = getattr(self, f"_construct_{kind}_colname")(label, direction)
        obj["name"][direction] = name
    else:
      name = getattr(self, f"_construct_{kind}_colname")(label)
      obj = {"type": kind, "directed": False, "name": name}
    return obj

  def _construct_index_colname(self, label = None, direction = None):
    pre = self.naming_config["index_prefix"]
    suf = self.naming_config["index_suffix"]
    if label is None:
      label = ""
    else:
      label = f":{label}"
    if direction is None:
      dpre = ""
      dsuf = ""
    else:
      dpre = self.naming_config[f"{direction}_prefix"]
      dsuf = self.naming_config[f"{direction}_suffix"]
    return f"{dpre}{pre}{self.profile.name}{suf}{label}{dsuf}"

  def _construct_attribute_colname(self, label, direction = None):
    pre = self.naming_config["attribute_prefix"]
    suf = self.naming_config["attribute_suffix"]
    if direction is None:
      dpre = ""
      dsuf = ""
    else:
      dpre = self.naming_config[f"{direction}_prefix"]
      dsuf = self.naming_config[f"{direction}_suffix"]
    return f"{dpre}{pre}{label}{suf}{dsuf}"

  def _read_from_network(self, metadata, network):
    if metadata["directed"]:
      data = {}
      for direction in ["forward", "backward"]:
        name = metadata["name"][direction]
        data[direction] = nx.get_edge_attributes(network, name)
        if not data[direction]:
          raise NetapyNetworkError(f"Cannot find network attribute '{name}'")
    else:
      name = metadata["name"]
      data = nx.get_edge_attributes(network, name)
      if not data:
        raise NetapyNetworkError(f"Cannot find attribute '{name}'")
    return {"data": data}

  def _write_to_network(self, data, metadata, network):
    if metadata["directed"]:
      for direction in ["forward", "backward"]:
        dir_data = data["data"][direction]
        dir_name = metadata["name"][direction]
        nx.set_edge_attributes(network, dir_data, dir_name)
    else:
      nx.set_edge_attributes(network, data["data"], metadata["name"])

  def _extract_value(self, obj, edge_idx, direction = None):
    if obj["directed"]:
      data = obj["data"][direction]
    else:
      data = obj["data"]
    try:
      value = data[edge_idx]
    except KeyError:
      value = float("nan")
    if value is None:
      value = float("nan")
    return value

  def _index_edge(self, idx, direction, digits = 2):
    weights = self.profile.parsed["weights"]
    extractor = lambda obj: self._extract_value(obj, idx, direction)
    vals = [extractor(self._subindex_cache[i]) * weights[i] for i in weights]
    return round(sum(vals) / sum(weights.values()), digits)

  def _subindex_edge(self, idx, label, mapping, direction = None):
    value = self._extract_value(self._attribute_cache[label], idx, direction)
    for condition, assignment in mapping["rules"].items():
      if condition(value):
        if isinstance(assignment, dict):
          # TODO: What if subindex is directed but attribute not (or vice versa)?
          label = assignment["indicator"]
          value = self._extract_value(self._attribute_cache[label], idx, direction)
          return self._apply_indicator_mapping(value, assignment)
        else:
          return assignment
    return mapping["default"]

  def _is_directed_attribute(self, label):
    deriver = getattr(self, f"derive_{label}")
    return "direction" in inspect.getfullargspec(deriver)[0]