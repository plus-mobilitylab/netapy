import networkx as nx
import pandas as pd
import copy
import inspect
import logging
import random
import warnings

from abc import abstractmethod

from netapy import defaults, utils
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

  def __init__(self, profile, naming_config = None, fetch_layers = True):
    self.profile = profile
    if naming_config is None:
      self.naming_config = defaults.NETASCORE_NAMING_CONFIG
    else:
      self.naming_config = naming_config
    self.fetch_layers = fetch_layers
    self._subindex_cache = {}
    self._attribute_cache = {}
    self._use_attribute_cache = False

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

  @property
  def fetch_layers(self):
    return self._fetch_layers

  @fetch_layers.setter
  def fetch_layers(self, value):
    self._fetch_layers = value

  def run(self, network, **config):
    return self.generate_index(network, **config)

  def clean(self, network, **config):
    # TODO: Create workflow to remove all netascore columns from network.
    raise NotImplementedError()

  def generate_index(self, network, digits = 2, read = False, write = True,
                     read_subs = None, write_subs = None, read_attrs = None,
                     write_attrs = None, ignore_nodata = False,
                     compute_robustness = False):
    obj = self._init_metadata(kind = "index", directed = True)
    # Read values from the network if read = True and the index exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the indices by taking a weighted average of subindices.
    if not obj["data"]:
      config = {
        "read": read if read_subs is None else read_subs,
        "write": write if write_subs is None else write_subs,
        "read_attrs": read if read_attrs is None else read_attrs,
        "write_attrs": write if write_attrs is None else write_attrs
      }
      self._subindex_cache = self.generate_subindices(network, **config)
      edges = network.edges
      indexer = lambda e, d: self._index_edge(e, d, digits, ignore_nodata, compute_robustness)
      for direction in ["forward", "backward"]:
        obj["data"][direction] = {e:indexer(e, direction) for e in edges}
      self._subindex_cache.clear()
      # Write derived indices to the network if write = True.
      if write:
        if compute_robustness:
          idx_obj = copy.deepcopy(obj)
          rob_obj = copy.deepcopy(obj)
          for direction in ["forward", "backward"]:
            idx_obj["data"][direction] = {k:v[0] for k, v in obj["data"][direction].items()}
            rob_obj["data"][direction] = {k:v[1] for k, v in obj["data"][direction].items()}
            rob_obj["name"][direction] = self._construct_index_colname("robustness", direction)
          self._write_to_network(idx_obj, network)
          self._write_to_network(rob_obj, network)
        else:
          self._write_to_network(obj, network)
    return obj

  def generate_subindices(self, network, read = False, write = True,
                          read_attrs = None, write_attrs = None):
    out = {}
    config = {
      "read": read,
      "write": write,
      "read_attrs": read_attrs,
      "write_attrs": write_attrs,
      "clear_cache": False
    }
    for i in self.profile.parsed["weights"]:
      out[i] = self.generate_subindex(i, network, **config)
    self._attribute_cache.clear()
    return out

  def generate_subindex(self, label, network, read = False, write = True,
                        read_attrs = None, write_attrs = None, clear_cache = True):
    obj = self._init_metadata(label, kind = "index", directed = None)
    # Read values from the network if read = True and the index exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the indices by mapping its corresponding attribute values.
    if not obj["data"]:
      # Fetch the values of the attribute belonging to the subindex.
      use_cache_default = self._use_attribute_cache
      self._use_attribute_cache = True
      config = {
        "read": read if read_attrs is None else read_attrs,
        "write": write if write_attrs is None else write_attrs,
        "read_deps": read if read_attrs is None else read_attrs,
        "write_deps": write if write_attrs is None else write_attrs
      }
      try:
        attr = self._attribute_cache[label]
      except KeyError:
        attr = self.generate_attribute(label, network, **config)
        self._attribute_cache[label] = attr
      # Update directionality of subindex based on the fetched attribute.
      directed = attr["directed"]
      obj["directed"] = directed
      if directed:
        del obj["name"]["undirected"]
      else:
        obj["name"] = obj["name"]["undirected"]
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
        for direction in ["forward", "backward"]:
          idxs = {e:indexer(e, direction) for e in attr["data"][direction]}
          obj["data"][direction] = idxs
      else:
        idxs = {e:indexer(e, None) for e in attr["data"]}
        obj["data"] = idxs
      # Reset attribute cache.
      if clear_cache:
        self._attribute_cache.clear()
      self._use_attribute_cache = use_cache_default
      # Write derived indices to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def generate_attribute(self, label, network, read = False, write = True, **kwargs):
    return getattr(self, f"derive_{label}")(network, read, write, **kwargs)

  def derive_access_car(self, network, read = False, write = True, **kwargs):
    label = "access_car"
    obj = self._init_metadata(label, kind = "attribute", directed = True)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # TODO: Implement derivation workflow (below is just a placeholder)
      keys = network.edges
      pool = [True, True, True, True, False]
      vals = random.choices(pool, k = len(keys))
      data = {k:v for k, v in zip(keys, vals)}
      for direction in ["forward", "backward"]:
        obj["data"][direction] = data
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_access_bicycle(self, network, read = False, write = True, **kwargs):
    label = "access_bicycle"
    obj = self._init_metadata(label, kind = "attribute", directed = True)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # TODO: Implement derivation workflow (below is just a placeholder)
      keys = network.edges
      pool = [True, True, True, True, False]
      vals = random.choices(pool, k = len(keys))
      data = {k:v for k, v in zip(keys, vals)}
      for direction in ["forward", "backward"]:
        obj["data"][direction] = data
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_access_pedestrian(self, network, read = False, write = True, **kwargs):
    label = "access_pedestrian"
    obj = self._init_metadata(label, kind = "attribute", directed = True)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # TODO: Implement derivation workflow (below is just a placeholder)
      keys = network.edges
      pool = [True, True, True, True, False]
      vals = random.choices(pool, k = len(keys))
      data = {k:v for k, v in zip(keys, vals)}
      for direction in ["forward", "backward"]:
        obj["data"][direction] = data
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_bridge(self, network, read = False, write = True, **kwargs):
    label = "bridge"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      osm_bridge = nx.get_edge_attributes(network, "bridge")
      obj["data"] = {k:pd.notnull(v) for k, v in osm_bridge.items()}
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_stairs(self, network, read = False, write = True, **kwargs):
    label = "stairs"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      osm_highway = nx.get_edge_attributes(network, "highway")
      obj["data"] = {k:v == "steps" for k, v in osm_highway.items()}
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_tunnel(self, network, read = False, write = True, **kwargs):
    label = "tunnel"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      osm_tunnel = nx.get_edge_attributes(network, "tunnel")
      obj["data"] = {k:pd.notnull(v) for k, v in osm_tunnel.items()}
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_bicycle_infrastructure(self, network, read = False, write = True, **kwargs):
    label = "bicycle_infrastructure"
    obj = self._init_metadata(label, kind = "attribute", directed = True)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # Fetch input data.
      labs = ["highway", "cycleway", "bicycle", "foot"]
      data = network._get_edge_attributes(network, *labs)
      # Derive attribute values for each street segment from the input data.
      def set_value(x, direction):
        # Categorize the street segment.
        is_bikepath = x["highway"] == "cycleway" or x["cycleway"] == "track"
        is_footpath = x["highway"] == "footway"
        is_bikelane = x["cycleway"] in ["lane", "shared_lane"]
        is_buslane = x["cycleway"] == "share_busway"
        is_path = x["highway"] = "path"
        is_track = x["highway"] == "track"
        can_walk = x["foot"] in ["yes", "designated"]
        can_bike = x["bicycle"] in ["yes", "designated"]
        # First option: "bicycle_way"
        conditions = [
          is_bikepath and not can_walk,
          can_bike and not can_walk and not is_footpath,
        ]
        if any(conditions):
          return "bicycle_way"
        # Second option: "mixed_way"
        conditions = [
          is_bikepath and can_walk,
          is_footpath and can_bike,
          is_path and can_bike and can_walk,
          is_track and can_bike and can_walk,
        ]
        if any(conditions):
          return "mixed_way"
        # Third option: "bicycle_lane"
        if is_bikelane:
          return "bicycle_lane"
        # Fourth option: "bus_lane"
        if is_buslane:
          return "bus_lane"
        # Fallback option: "no"
        return "no"
      for direction in ["forward", "backward"]:
        vals = {x[0]:set_value(x[1], direction) for x in data.iterrows()}
        obj["data"][direction] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_pedestrian_infrastructure(self, network, read = False, write = True,
                                       read_deps = None, write_deps = None, **kwargs):
    label = "pedestrian_infrastructure"
    obj = self._init_metadata(label, kind = "attribute", directed = True)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # Fetch input data.
      # In this case this a combination of OSM attributes and derived attributes.
      osmlabs = ["highway", "cycleway", "bicycle", "foot"]
      osmdata = network._get_edge_attributes(*osmlabs)
      derlabs = [("access_pedestrian", "forward"), ("access_pedestrian", "backward")]
      derconf = {"read": read if read_deps is None else read_deps,
                 "write": write if write_deps is None else write_deps}
      derdata = self._get_derived_attributes(network, derlabs, **derconf)
      data = osmdata.join(derdata)
      # Derive attribute values from the input data.
      def set_value(x, direction):
        is_footarea = x["highway"] == "pedestrian"
        is_bikepath = x["highway"] == "cycleway" or x["cycleway"] == "track"
        is_footpath = x["highway"] == "footway"
        is_path = x["highway"] = "path"
        is_track = x["highway"] == "track"
        is_stairs = x["highway"] == "steps"
        can_walk = x["foot"] in ["yes", "designated"]
        can_bike = x["bicycle"] in ["yes", "designated"]
        access = x[("access_pedestrian", direction)]
        # First option: "pedestrian_area"
        if is_footarea:
          return "pedestrian_area"
        # Second option: "pedestrian_way"
        if is_footpath and not can_bike:
          return "pedestrian_way"
        # Third option: "mixed_way"
        conditions = [
          is_bikepath and can_walk,
          is_footpath and can_bike,
          is_path and can_bike and can_walk,
          is_track and can_bike and can_walk,
        ]
        if any(conditions):
          return "mixed_way"
        # Fourth option: "stairs"
        if is_stairs:
          return "stairs"
        # Fifth option: "sidewalk"
        if access:
          return "sidewalk"
        # Fallback option: "no"
        return "no"
      for direction in ["forward", "backward"]:
        vals = {x[0]:set_value(x[1], direction) for x in data.iterrows()}
        obj["data"][direction] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_gradient(self, network, read = False, write = True, **kwargs):
    label = "gradient"
    obj = self._init_metadata(label, kind = "attribute", directed = True)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # Fetch input data.
      labs = ["grade"]
      data = network._get_edge_attributes(*labs)
      if all([pd.isnull(x) for x in data["grade"]]):
        warnings.warn(
          "Network does not contain values for edge attribute 'grade'."
          "Did you run network.write_elevation()?"
        )
      # Derive attribute values from the input data.
      grade_mapping = {
        lambda x: x < -0.12: -4,
        lambda x: -0.12 <= x < -0.06: -3,
        lambda x: -0.06 <= x < -0.03: -2,
        lambda x: -0.03 <= x < -0.015: -1,
        lambda x: -0.015 <= x <= 0.015: 0,
        lambda x: 0.015 < x <= 0.03: 1,
        lambda x: 0.03 < x <= 0.06: 2,
        lambda x: 0.06 < x <= 0.12: 3,
        lambda x: x > 0.12: 4
      }
      def set_value(x):
        g = x["grade"]
        if pd.isnull(g):
          return float("nan")
        for k, v in grade_mapping.items():
          if k(g):
            return v
        return float("nan")
      vals = {x[0]:set_value(x[1]) for x in data.iterrows()}
      obj["data"]["forward"] = vals
      obj["data"]["backward"] = {k:v * -1 for k, v in vals.items()}
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_max_speed(self, network, read = False, write = True, **kwargs):
    label = "max_speed"
    obj = self._init_metadata(label, kind = "attribute", directed = True)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # TODO: Implement derivation workflow (below is just a placeholder)
      keys = network.edges
      pool = range(0, 130)
      vals = random.choices(pool, k = len(keys))
      data = {k:v for k, v in zip(keys, vals)}
      for direction in ["forward", "backward"]:
        obj["data"][direction] = data
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_max_speed_greatest(self, network, read = False, write = True, **kwargs):
    label = "max_speed_greatest"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # TODO: Implement derivation workflow (below is just a placeholder)
      keys = network.edges
      pool = range(0, 130)
      vals = random.choices(pool, k = len(keys))
      obj["data"] = {k:v for k, v in zip(keys, vals)}
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_road_category(self, network, read = False, write = True, **kwargs):
    label = "road_category"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # Fetch input data.
      labs = ["highway", "access", "bicycle", "foot", "motor_vehicle",
              "maxspeed", "tracktype", "surface"]
      data = network._get_edge_attributes(network, *labs)
      # Convert maxspeed values to numeric.
      # TODO: How to handle different units of maxspeed?
      nums = [utils.split_string(x, split_nodata = True)[1] for x in data["maxspeed"]]
      data["maxspeed"] = nums
      # Derive attribute values for each street segment from the input data.
      def set_value(x):
        # First option: "primary"
        C1 = x["highway"] in ["primary", "primary_link"]
        if C1:
          return "primary"
        # Second option: "secondary"
        C1 = x["highway"] in ["secondary", "secondary_link"]
        C2 = x["highway"] == "unclassified"
        C3 = 80 <= x["maxspeed"] <= 100
        if C1 or (C2 and C3):
          return "secondary"
        # Third option: "residential"
        C1 = x["highway"] in ["tertiary", "tertiary_link", "residential"]
        C2 = x["highway"] == "unclassified"
        C3 = x["maxspeed"] < 80 or x["maxspeed"] > 100
        C4 = pd.isnull(x["motor_vehicle"]) or x["motor_vehicle"] in ["yes", "designated"]
        if (C1 or (C2 and C3)) and C4:
          return "residential"
        # Fourth option: "service"
        C1 = x["highway"] in ["service", "living_street"]
        C2 = x["motor_vehicle"] in ["agricultural", "forestry"]
        C3 = pd.isnull(x["access"]) or x["access"] != "no"
        C4 = x["highway"] == "path"
        C5 = x["highway"] == "track"
        C6 = pd.isnull(x["tracktype"]) or x["tracktype"] in ["grade1", "grade2"]
        C7 = pd.isnull(x["motor_vehicle"]) or x["motor_vehicle"] != "no"
        if C1 or (C2 and C3) or (C4 and C3) or (C5 and C3 and C6 and C7):
          return "service"
        # Fifth option: "calmed"
        C1 = x["motor_vehicle"] in ["delivery", "destination", "private"]
        C2 = x["highway"] == "track"
        C3 = x["tracktype"] in ["grade3", "grade4", "grade5"]
        C4 = x["surface"] in ["paved", "gravel", "asphalt"]
        if C1 or (C2 and C3 and C4):
          return "calmed"
        # Sixth option: "no_mit"
        C1 = x["highway"] in ["footway", "cycleway"]
        C2 = x["motor_vehicle"] == "no"
        C3 = pd.isnull(x["bicycle"]) or x["bicycle"] != "no"
        C4 = pd.notnull(x["access"]) and x["access"] != "yes"
        if C1 or (C2 and C3) or (C4 and C3):
          return "no_mit"
        # Seventh option: "path"
        C1 = x["highway"] == "path"
        C2 = x["foot"] == "yes"
        C3 = x["bicycle"] not in ["yes", "designated"]
        C4 = x["highway"] == "steps"
        C5 = x["highway"] == "track"
        C6 = x["tracktype"] in ["grade3", "grade4", "grade5"]
        C7 = x["surface"] not in ["paved", "gravel", "asphalt"]
        if (C1 and C2 and C3) or C4 or (C5 and C6 and C7):
          return "path"
        return None
      vals = {x[0]:set_value(x[1]) for x in data.iterrows()}
      obj["data"] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_designated_route(self, network, read = False, write = True, **kwargs):
    label = "designated_route"
    obj = self._init_metadata(label, kind = "attribute", directed = True)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # NOTE: Derivation of this attribute is not implemented.
      # This is because osmnx cannot query relations consisting of ways.
      labs = ["route"]
      data = network._get_edge_attributes(network, *labs)
      def set_value(x, direction):
        warnings.warn(f"Derivation of attribute '{label}' is not yet implemented")
        return None
      for direction in ["forward", "backward"]:
        vals = {x[0]:set_value(x[1], direction) for x in data.iterrows()}
        obj["data"][direction] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_number_lanes(self, network, read = False, write = True, **kwargs):
    label = "number_lanes"
    obj = self._init_metadata(label, kind = "attribute", directed = True)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # Fetch input data.
      labs = ["lanes", "lanes:forward", "lanes:backward"]
      data = network._get_edge_attributes(network, *labs)
      # Derive attribute values for each street segment from the input data.
      def set_value(x, direction):
        directed_lanes = x[f"lanes:{direction}"]
        undirected_lanes = x["lanes"]
        if pd.notnull(directed_lanes):
          return float(directed_lanes)
        if pd.notnull(undirected_lanes):
          return float(undirected_lanes)
        return float("nan")
      for direction in ["forward", "backward"]:
        vals = {x[0]:set_value(x[1], direction) for x in data.iterrows()}
        obj["data"][direction] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_width(self, network, read = False, write = True, **kwargs):
    label = "width"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # Fetch input data.
      labs = ["width"]
      data = network._get_edge_attributes(network, *labs)
      # Derive attribute values for each street segment from the input data.
      def set_value(x):
        # TODO: How to handle different units of width?
        return utils.split_string(x["width"], split_nodata = True)[1]
      vals = {x[0]:set_value(x[1]) for x in data.iterrows()}
      obj["data"] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_pavement(self, network, read = False, write = True, **kwargs):
    label = "pavement"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # Fetch input data.
      labs = ["surface"]
      data = network._get_edge_attributes(network, *labs)
      # Derive attribute values for each street segment from the input data.
      def set_value(x):
        # First option: "asphalt"
        tags = ["asphalt", "paved", "concrete"]
        if x["surface"] in tags:
          return "asphalt"
        # Second option: "gravel"
        tags = ["compacted", "fine_gravel", "gravel", "paving_stones",
                "pebblestone", "ground;gravel", "unpaved"]
        if x["surface"] in tags:
          return "gravel"
        # Third option: "soft"
        tags = ["dirt", "earth", "grass", "ground", "ground;grass", "sand", "wood"]
        if x["surface"] in tags:
          return "soft"
        # Fourth option: "cobble"
        tags = ["cobblestone"]
        if x["surface"] in tags:
          return "cobble"
        # Fallback option: None
        return None
      vals = {x[0]:set_value(x[1]) for x in data.iterrows()}
      obj["data"] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_parking(self, network, read = False, write = True, **kwargs):
    label = "parking"
    obj = self._init_metadata(label, kind = "attribute", directed = True)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      # NOTE: Derivation of this attribute is not implemented.
      # This is because there is no implementation of it yet in the NetAScore core.
      labs = ["parking"]
      data = network._get_edge_attributes(network, *labs)
      def set_value(x, direction):
        warnings.warn(f"Derivation of attribute '{label}' is not yet implemented")
        return None
      for direction in ["forward", "backward"]:
        vals = {x[0]:set_value(x[1], direction) for x in data.iterrows()}
        obj["data"][direction] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_crossings(self, network, read = False, write = True, **kwargs):
    label = "crossings"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      network._check_layer_presence("crossings", fetch = self.fetch_layers)
      edges = network._get_edge_geometries(projected = True)
      layer = network._get_layer_geometries("crossings", projected = True)
      def set_value(x):
        buffer = x.buffer(distance = 10, cap_style = 2)
        return buffer.intersects(layer).sum()
      vals = {x[0]:set_value(x[1]) for x in edges.items()}
      obj["data"] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_buildings(self, network, read = False, write = True, **kwargs):
    label = "buildings"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      network._check_layer_presence("buildings", fetch = self.fetch_layers)
      edges = network._get_edge_geometries(projected = True)
      layer = network._get_layer_geometries("buildings", projected = True)
      def set_value(x):
        buffer = x.buffer(distance = 20, cap_style = 2)
        intersection = buffer.intersection(layer)
        proportion = round(intersection.area.sum() / buffer.area * 100, 1)
        return min(proportion, 100)
      vals = {x[0]:set_value(x[1]) for x in edges.items()}
      obj["data"] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_facilities(self, network, read = False, write = True, **kwargs):
    label = "facilities"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      network._check_layer_presence("facilities", fetch = self.fetch_layers)
      edges = network._get_edge_geometries(projected = True)
      layer = network._get_layer_geometries("facilities", projected = True)
      def set_value(x):
        buffer = x.buffer(distance = 10, cap_style = 2)
        return buffer.intersects(layer).sum()
      vals = {x[0]:set_value(x[1]) for x in edges.items()}
      obj["data"] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_greenness(self, network, read = False, write = True, **kwargs):
    label = "greenness"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      network._check_layer_presence("greenness", fetch = self.fetch_layers)
      edges = network._get_edge_geometries(projected = True)
      layer = network._get_layer_geometries("greenness", projected = True)
      def set_value(x):
        buffer = x.buffer(distance = 30, cap_style = 2)
        intersection = buffer.intersection(layer)
        proportion = round(intersection.area.sum() / buffer.area * 100, 1)
        return min(proportion, 100)
      vals = {x[0]:set_value(x[1]) for x in edges.items()}
      obj["data"] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_water(self, network, read = False, write = True, **kwargs):
    label = "water"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      network._check_layer_presence("water", fetch = self.fetch_layers)
      edges = network._get_edge_geometries(projected = True)
      layer = network._get_layer_geometries("water", projected = True)
      def set_value(x):
        buffer = x.buffer(distance = 30, cap_style = 2)
        return buffer.intersects(layer).sum() > 0
      vals = {x[0]:set_value(x[1]) for x in edges.items()}
      obj["data"] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def derive_noise(self, network, read = False, write = True, **kwargs):
    label = "noise"
    obj = self._init_metadata(label, kind = "attribute", directed = False)
    # Read values from the network if read = True and the attribute exists.
    if read:
      self._read_from_network(obj, network)
    # Otherwise derive the attribute values from the network data.
    if not obj["data"]:
      network._check_layer_presence("noise", fetch = False)
      edges = network._get_edge_geometries(projected = True)
      polys = network._get_layer_geometries("noise", projected = True)
      noise = network._get_layer_attributes("noise", "noise")
      def set_value(x):
        lengths = x.intersection(polys).length
        intersects = lengths > 0
        weights = lengths[intersects]
        values = noise[intersects] * weights
        return round(sum(values) / sum(weights), 0)
      vals = {x[0]:set_value(x[1]) for x in edges.items()}
      obj["data"] = vals
      # Write derived attributes to the network if write = True.
      if write:
        self._write_to_network(obj, network)
    return obj

  def _init_metadata(self, label = None, kind = "attribute", directed = False):
    # If directionality is not defined:
    # --> Create column names for both undirected and directed forms.
    if directed is None:
      obj = {"type": kind, "directed": None, "name": {}, "data": {}}
      for direction in ["forward", "backward"]:
        name = getattr(self, f"_construct_{kind}_colname")(label, direction)
        obj["name"][direction] = name
      name = getattr(self, f"_construct_{kind}_colname")(label)
      obj["name"]["undirected"] = name
    # Initialize metadata for directed data.
    elif directed:
      obj = {"type": kind, "directed": True, "name": {}, "data": {}}
      for direction in ["forward", "backward"]:
        name = getattr(self, f"_construct_{kind}_colname")(label, direction)
        obj["name"][direction] = name
    # Initialize metadata for undirected data.
    else:
      name = getattr(self, f"_construct_{kind}_colname")(label)
      obj = {"type": kind, "directed": False, "name": name, "data": {}}
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

  def _read_from_network(self, obj, network):
    # If directionality is unknown:
    # --> Try if network attribute is present in undirected or directed form.
    if obj["directed"] is None:
      undirected_obj = copy.deepcopy(obj)
      undirected_obj["directed"] = False
      undirected_obj["name"] = obj["name"]["undirected"]
      self._read_from_network(undirected_obj, network)
      if not undirected_obj["data"]:
        directed_obj = copy.deepcopy(obj)
        directed_obj["directed"] = True
        del directed_obj["name"]["undirected"]
        self._read_from_network(directed_obj, network)
        if not directed_obj["data"]:
          return obj
        else:
          return directed_obj
      else:
        return undirected_obj
    # Read directed network attribute.
    if obj["directed"]:
      data = {}
      for direction in ["forward", "backward"]:
        name = obj["name"][direction]
        data[direction] = nx.get_edge_attributes(network, name)
        if not data[direction]:
          data = {}
          break
    # Read undirected network attribute.
    else:
      name = obj["name"]
      data = nx.get_edge_attributes(network, name)
    obj["data"] = data

  def _write_to_network(self, obj, network):
    if obj["directed"] is None:
      raise ValueError("Cannot write network attribute with unknown directionality")
    if obj["directed"]:
      for direction in ["forward", "backward"]:
        dir_data = obj["data"][direction]
        dir_name = obj["name"][direction]
        nx.set_edge_attributes(network, dir_data, dir_name)
    else:
      nx.set_edge_attributes(network, obj["data"], obj["name"])

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

  def _index_edge(self, idx, direction, digits = 2, ignore_nodata = False,
                  compute_robustness = False):
    indicators = self.profile.parsed["weights"]
    extractor = lambda obj: self._extract_value(obj, idx, direction)
    if ignore_nodata:
      values = []
      weights = []
      for i, x in indicators.items():
        subindex = extractor(self._subindex_cache[i])
        if pd.notnull(subindex):
          values.append(subindex * x)
          weights.append(x)
    else:
      values = [extractor(self._subindex_cache[i]) * x for i, x in indicators.items()]
      weights = indicators.values()
    try:
      indexvalue = round(sum(values) / sum(weights), digits)
    except ZeroDivisionError:
      indexvalue = float("nan")
    if compute_robustness:
      try:
        robustness = sum(weights) / sum(indicators.values())
      except ZeroDivisionError:
        robustness = float("nan")
      return indexvalue, robustness
    else:
      return indexvalue

  def _subindex_edge(self, idx, label, mapping, direction = None):
    value = self._extract_value(self._attribute_cache[label], idx, direction)
    for condition, assignment in mapping["rules"].items():
      if condition(value):
        if isinstance(assignment, dict):
          # TODO: What if subindex is directed but attribute not (or vice versa)?
          label = assignment["indicator"]
          return self._subindex_edge(idx, label, assignment, direction)
        else:
          return assignment
    return mapping["default"]

  def _get_derived_attributes(self, network, attrs, read = True, write = True):
    if network.is_multigraph():
      E = network.edges(keys = True)
    else:
      E = network.edges()
    A = {}
    for x in attrs:
      if isinstance(x, tuple):
        directed = True
        d = x[1]
        x = x[0]
      else:
        directed = False
      if self._use_attribute_cache:
        try:
          a = self._attribute_cache[x]
        except KeyError:
          a = self.generate_attribute(x, network, read = read, write = write)
          self._attribute_cache[x] = a
      else:
        a = self.generate_attribute(x, network, read = read, write = write)
      if directed:
        A[(x, d)] = a["data"][d]
      else:
        A[x] = a_data
    E = [[e, {k:A[k][e] for k in attrs if e in A[k]}] for e in E]
    keys, data = zip(*E)
    return pd.DataFrame(data, index = keys)