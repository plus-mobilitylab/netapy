import networkx as nx
import copy
import random

from abc import abstractmethod

from netapy import defaults
from netapy.profiles import NetascoreProfile
from netapy.exceptions import NetapyNetworkError


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

  def generate_index(self, network, digits = 2, read = False, write = True,
                     read_subs = False, write_subs = True, read_attrs = False,
                     write_attrs = True, return_data = False):
    metadata = self._init_metadata(kind = "index", directed = True)
    if read:
      try:
        data = self._read_from_network(metadata, network)
      except NetapyNetworkError:
        pass
      else:
        if return_data:
          metadata.update(data)
        return metadata
    config = {
      "read": read_subs,
      "write": write_subs,
      "read_attrs": read_attrs,
      "write_attrs": write_attrs,
      "return_data": True
    }
    subindices = self.generate_subindices(network, **config)
    edges = network.edges
    data = {"data": {}}
    indexer = lambda e, d: self._index_edge(e, d, subindices, digits)
    for direction in ["forward", "backward"]:
      data["data"][direction] = {e:indexer(e, direction) for e in edges}
    if write:
      self._write_to_network(data, metadata, network)
    if return_data:
      metadata.update(data)
    return metadata

  def generate_subindices(self, network, read = False, write = True,
                          read_attrs = False, write_attrs = True, return_data = False):
    sources = {}
    indices = {}
    config = {"read": read_attrs, "write": write_attrs, "return_data": True}
    for i in self.profile.parsed["weights"]:
      src_meta = getattr(self, f"init_{i}")(network)
      directed = src_meta["directed"]
      idx_meta = self._init_metadata(i, kind = "index", directed = directed)
      if read:
        try:
          idx_data = self._read_from_network(idx_meta, network)
        except NetapyNetworkError:
          generate = True
        else:
          if return_data:
            idx_meta.update(idx_data)
          indices[i] = idx_meta
          generate = False
      else:
        generate = True
      if generate:
        sources[i] = self._derive_from_network(src_meta, network, **config)
        mapping = self.profile.parsed["indicator_mapping"][i]
        if directed:
          idx_data = {"data": {}}
          for direction in ["forward", "backward"]:
            src = sources[i]["data"][direction].items()
            idx = {k:self._apply_indicator_mapping(v, mapping) for k, v in src}
            idx_data["data"][direction] = idx
        else:
          idx_data = {}
          src = sources[i]["data"].items()
          idx = {k:self._apply_indicator_mapping(v, mapping) for k, v in src}
          idx_data["data"] = idx
        if write:
          self._write_to_network(idx_data, idx_meta, network)
        if return_data:
          idx_meta.update(idx_data)
        indices[i] = idx_meta
    return indices

  def derive_attribute(self, label, network, read = False, write = True,
                       return_data = False):
    metadata = getattr(self, f"init_{label}")(network)
    return self._derive_from_network(metadata, network, read, write, return_data)

  def init_access_car(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network, direction):
      keys = network.edges
      pool = [True, True, True, True, False]
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("access_car", kind = "attribute", directed = True)
    meta["deriver"] = deriver
    return meta

  def init_access_bicycle(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network, direction):
      keys = network.edges
      pool = [True, True, True, True, False]
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("access_bicycle", kind = "attribute", directed = True)
    meta["deriver"] = deriver
    return meta

  def init_access_pedestrian(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network, direction):
      keys = network.edges
      pool = [True, True, True, True, False]
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("access_pedestrian", kind = "attribute", directed = True)
    meta["deriver"] = deriver
    return meta

  def init_bridge(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      vals = [False] * len(keys)
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("bridge", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

  def init_tunnel(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      vals = [False] * len(keys)
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("tunnel", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

  def init_bicycle_infrastructure(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network, direction):
      keys = network.edges
      pool = ["bicycle_way", "mixed_way", "bicycle_lane", "bus_lane", "no"]
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("bicycle_infrastructure", kind = "attribute", directed = True)
    meta["deriver"] = deriver
    return meta

  def init_pedestrian_infrastructure(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network, direction):
      keys = network.edges
      pool = ["pedestrian_area", "pedestrian_way", "mixed_way", "stairs", "sidewalk", "no"]
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("pedestrian_infrastructure", kind = "attribute", directed = True)
    meta["deriver"] = deriver
    return meta

  def init_gradient(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network, direction):
      keys = network.edges
      pool = [-4, -3, -2, -1, 0, 1, 2, 3, 4]
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("gradient", kind = "attribute", directed = True)
    meta["deriver"] = deriver
    return meta

  def init_max_speed(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network, direction):
      keys = network.edges
      pool = range(0, 130)
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("max_speed", kind = "attribute", directed = True)
    meta["deriver"] = deriver
    return meta

  def init_max_speed_greatest(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      pool = range(0, 130)
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("max_speed_greatest", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

  def init_road_category(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      pool = ["primary", "secondary", "residential", "service", "calmed", "no_mit", "path"]
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("road_category", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

  def init_designated_route(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network, direction):
      keys = network.edges
      pool = ["local", "regional", "national", "international", "unknown", "no"]
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("designated_route", kind = "attribute", directed = True)
    meta["deriver"] = deriver
    return meta

  def init_number_lanes(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network, direction):
      keys = network.edges
      pool = range(0, 10)
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("number_lanes", kind = "attribute", directed = True)
    meta["deriver"] = deriver
    return meta

  def init_width(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      pool = range(0, 10)
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("width", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

  def init_pavement(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      pool = ["asphalt", "gravel", "cobble", "soft"]
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("pavement", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

  def init_parking(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network, direction):
      keys = network.edges
      pool = ["yes", "no"]
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("parking", kind = "attribute", directed = True)
    meta["deriver"] = deriver
    return meta

  def init_crossings(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      pool = range(0, 10)
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("crossings", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

  def init_buildings(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      pool = range(0, 100)
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("buildings", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

  def init_facilities(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      pool = range(0, 10)
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("facilities", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

  def init_greenness(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      pool = range(0, 100)
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("greenness", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

  def init_water(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      pool = [True, False]
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("water", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

  def init_noise(self, network):
    # TODO: Implement deriver function (below is just a placeholder)
    def deriver(network):
      keys = network.edges
      pool = range(0, 100)
      vals = random.choices(pool, k = len(keys))
      return {k:v for k, v in zip(keys, vals)}
    meta = self._init_metadata("noise", kind = "attribute", directed = False)
    meta["deriver"] = deriver
    return meta

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

  def _derive_from_network(self, metadata, network, read = False, write = True,
                           return_data = False):
    if read:
      try:
        data = self._read_from_network(metadata, network)
      except NetapyNetworkError:
        pass
      else:
        if return_data:
          metadata.update(data)
        return metadata
    if metadata["directed"]:
      data = {"data": {}}
      for direction in ["forward", "backward"]:
        data["data"][direction] = metadata["deriver"](network, direction)
    else:
      data = {}
      data["data"] = metadata["deriver"](network)
    if write:
      self._write_to_network(data, metadata, network)
    if return_data:
      metadata.update(data)
    return metadata

  def _extract_value(self, obj, edge_idx, direction):
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

  def _index_edge(self, idx, direction, subindices, digits = 2):
    weights = self.profile.parsed["weights"]
    extractor = lambda obj: self._extract_value(obj, idx, direction)
    vals = [extractor(subindices[i]) * weights[i] for i in weights]
    return round(sum(vals) / sum(weights.values()), digits)

  def _apply_indicator_mapping(self, value, mapping):
    rules = mapping["rules"]
    for condition, assignment in rules.items():
      if condition(value):
        # TODO: Handle assignments that are nested indicator mappings.
        return assignment
    return mapping["default"]