import osmnx as ox
import copy

from abc import abstractmethod
from networkx import MultiDiGraph

from netapy import defaults


class Network(MultiDiGraph):

  def __init__(self, obj):
    super(Network, self).__init__(obj)

  @abstractmethod
  def assess(self, assessor, inplace = True):
    pass


class NetascoreNetwork(Network):
    
  def __init__(self, obj, query_type, query_kwargs, buildings = False,
               facilities = False, greenness = False, water = False):
    super(NetascoreNetwork, self).__init__(obj)
    self._query_type = query_type
    self._query_kwargs = query_kwargs
    for layer in ["buildings", "facilities", "greenness", "water"]:
      if locals()[layer]:
        getattr(self, f"fetch_{layer}")()
      else:
        setattr(self, f"{layer}", None)

  @property
  def query_type(self):
    return self._query_type

  @property
  def query_kwargs(self):
    return self._query_kwargs

  @property
  def buildings(self):
    return self._buildings

  @buildings.setter
  def buildings(self, value):
    self._buildings = value

  @property
  def facilities(self):
    return self._facilities

  @facilities.setter
  def facilities(self, value):
    self._facilities = value

  @property
  def greenness(self):
    return self._greenness

  @greenness.setter
  def greenness(self, value):
    self._greenness = value

  @property
  def water(self):
    return self._water

  @water.setter
  def water(self, value):
    self._water = value

  @classmethod
  def from_place(cls, query, which_result = None, buffer_dist = None, **kwargs):
    DEFAULT_STREET_KEYS = ox.settings.useful_tags_way
    ox.settings.useful_tags_way = defaults.NETASCORE_STREET_KEYS
    qtype = "place"
    qkwargs = {
      "query": query,
      "network_type": "all",
      "which_result": which_result,
      "buffer_dist": buffer_dist
    }
    obj = cls(ox.graph_from_place(**qkwargs), qtype, qkwargs, **kwargs)
    ox.settings.useful_tags_way = DEFAULT_STREET_KEYS
    return obj

  @classmethod
  def from_polygon(cls, polygon, **kwargs):
    DEFAULT_STREET_KEYS = ox.settings.useful_tags_way
    ox.settings.useful_tags_way = defaults.NETASCORE_STREET_KEYS
    qtype = "polygon"
    qkwargs = {
      "polygon": polygon,
      "network_type": "all"
    }
    obj = cls(ox.graph_from_polygon(**qkwargs), qtype, qkwargs, **kwargs)
    ox.settings.useful_tags_way = DEFAULT_STREET_KEYS
    return obj

  @classmethod
  def from_point(cls, point, dist = 1000, **kwargs):
    DEFAULT_STREET_KEYS = ox.settings.useful_tags_way
    ox.settings.useful_tags_way = defaults.NETASCORE_STREET_KEYS
    qtype = "point"
    qkwargs = {
      "center_point": point,
      "network_type": "all",
      "dist": dist
    }
    obj = cls(ox.graph_from_point(**qkwargs), qtype, qkwargs, **kwargs)
    ox.settings.useful_tags_way = DEFAULT_STREET_KEYS
    return obj

  @classmethod
  def from_bbox(cls, coords, **kwargs):
    DEFAULT_STREET_KEYS = ox.settings.useful_tags_way
    ox.settings.useful_tags_way = defaults.NETASCORE_STREET_KEYS
    qtype = "bbox"
    qkwargs = {k:v for k, v in zip(["west", "south", "east", "north"], coords)}
    qkwargs["network_type"] = "all"
    obj = cls(ox.graph_from_bbox(**qkwargs), qtype, qkwargs, **kwargs)
    ox.settings.useful_tags_way = DEFAULT_STREET_KEYS
    return obj

  @classmethod
  def from_file(cls, filepath, **kwargs):
    # TODO: Create workflow to load street network from OSM file.
    raise NotImplementedError()

  def fetch_geometries(self, name, query):
    getattr(self, f"_fetch_geometries_from_{self.query_type}")(name, query)

  def _fetch_geometries_from_place(self, name, query):
    kws = ["query", "which_result", "buffer_dist"]
    kwargs = {k:v for k, v in self.query_kwargs.items() if k in kws}
    setattr(self, name, ox.geometries_from_place(tags = query, **kwargs))

  def _fetch_geometries_from_polygon(self, name, query):
    kws = ["polygon"]
    kwargs = {k:v for k, v in self.query_kwargs.items() if k in kws}
    setattr(self, name, ox.geometries_from_polygon(tags = query, **kwargs))

  def _fetch_geometries_from_point(self, name, query):
    kws = ["center_point", "dist"]
    kwargs = {k:v for k, v in self.query_kwargs.items() if k in kws}
    setattr(self, name, ox.geometries_from_point(tags = query, **kwargs))

  def _fetch_geometries_from_bbox(self, name, query):
    kws = ["west", "south", "east", "north"]
    kwargs = {k:v for k, v in self.query_kwargs.items() if k in kws}
    setattr(self, name, ox.geometries_from_bbox(tags = query, **kwargs))

  def _fetch_geometries_from_file(self, name, query):
    # TODO: Create workflow to load geometries from OSM file.
    raise NotImplementedError()

  def fetch_buildings(self):
    self.fetch_geometries("buildings", defaults.NETASCORE_BUILDINGS_QUERY)

  def fetch_crossings(self):
    self.fetch_geometries("crossings", defaults.NETASCORE_CROSSINGS_QUERY)

  def fetch_facilities(self):
    self.fetch_geometries("facilities", defaults.NETASCORE_FACILITIES_QUERY)

  def fetch_greenness(self):
    self.fetch_geometries("greenness", defaults.NETASCORE_GREENNESS_QUERY)

  def fetch_water(self):
    self.fetch_geometries("water", defaults.NETASCORE_WATER_QUERY)

  def add_elevation(self, filepath, inplace = True, **kwargs):
    enriched = ox.elevation.add_node_elevations_raster(self, filepath, **kwargs)
    if inplace:
      self.nodes = enriched.nodes
    else:
      return enriched

  def add_noise(self, filepath):
    # TODO: Create workflow to add noise and other additional data from files.
    raise NotImplementedError()

  def assess(self, assessor, inplace = True, print_output = False, **config):
    network = self if inplace else copy.deepcopy(self)
    out = assessor.run(network, **config)
    if print_output:
      print(out)
    if not inplace:
      return network
      