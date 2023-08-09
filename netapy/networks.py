import geopandas as gpd
import pandas as pd
import osmnx as ox
import copy
import logging

from abc import abstractmethod
from networkx import MultiDiGraph
from pyproj import CRS

from netapy import defaults

logger = logging.getLogger(__name__)

class Network(MultiDiGraph):

  def __init__(self, obj):
    super(Network, self).__init__(obj)

  @abstractmethod
  def assess(self, assessor, inplace = True):
    pass


class NetascoreNetwork(Network):
    
  def __init__(self, obj, query_type, query_kwargs, buildings = False,
               facilities = False, greenness = False, water = False,
               projected_crs = None):
    super(NetascoreNetwork, self).__init__(obj)
    self._query_type = query_type
    self._query_kwargs = query_kwargs
    for layer in ["buildings", "facilities", "greenness", "water"]:
      if locals()[layer]:
        getattr(self, f"fetch_{layer}")()
      else:
        setattr(self, f"{layer}", None)
    self.noise = None
    if projected_crs is None:
      self._projected_crs = self._get_node_geometries().estimate_utm_crs()
    else:
      self.projected_crs = projected_crs

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

  @property
  def noise(self):
    return self._noise

  @noise.setter
  def noise(self, value):
    self._noise = value

  @property
  def projected_crs(self):
    return self._projected_crs

  @projected_crs.setter
  def projected_crs(self, value):
    self._projected_crs = CRS.from_user_input(value)

  @classmethod
  def from_place(cls, query, which_result = None, **kwargs):
    DEFAULT_STREET_KEYS = ox.settings.useful_tags_way
    ox.settings.useful_tags_way = defaults.NETASCORE_STREET_KEYS
    qtype = "place"
    qkwargs = {
      "query": query,
      "network_type": "all",
      "simplify": False,
      "which_result": which_result
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
      "network_type": "all",
      "simplify": False
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
      "simplify": False,
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
    qkwargs["simplify"] = False
    obj = cls(ox.graph_from_bbox(**qkwargs), qtype, qkwargs, **kwargs)
    ox.settings.useful_tags_way = DEFAULT_STREET_KEYS
    return obj

  @classmethod
  def from_file(cls, filepath, **kwargs):
    # TODO: Create workflow to load street network from OSM file.
    raise NotImplementedError()

  def fetch_layer(self, name, query):
    getattr(self, f"_fetch_layer_from_{self.query_type}")(name, query)

  def _fetch_layer_from_place(self, name, query):
    kws = ["query", "which_result", "buffer_dist"]
    kwargs = {k:v for k, v in self.query_kwargs.items() if k in kws}
    setattr(self, name, ox.features_from_place(tags = query, **kwargs))

  def _fetch_layer_from_polygon(self, name, query):
    kws = ["polygon"]
    kwargs = {k:v for k, v in self.query_kwargs.items() if k in kws}
    setattr(self, name, ox.features_from_polygon(tags = query, **kwargs))

  def _fetch_layer_from_point(self, name, query):
    kws = ["center_point", "dist"]
    kwargs = {k:v for k, v in self.query_kwargs.items() if k in kws}
    setattr(self, name, ox.features_from_point(tags = query, **kwargs))

  def _fetch_layer_from_bbox(self, name, query):
    kws = ["west", "south", "east", "north"]
    kwargs = {k:v for k, v in self.query_kwargs.items() if k in kws}
    setattr(self, name, ox.features_from_bbox(tags = query, **kwargs))

  def _fetch_layer_from_file(self, name, query):
    # TODO: Create workflow to load geometries from OSM file.
    raise NotImplementedError()

  def fetch_buildings(self):
    self.fetch_layer("buildings", defaults.NETASCORE_BUILDINGS_QUERY)

  def fetch_crossings(self):
    self.fetch_layer("crossings", defaults.NETASCORE_CROSSINGS_QUERY)

  def fetch_facilities(self):
    self.fetch_layer("facilities", defaults.NETASCORE_FACILITIES_QUERY)

  def fetch_greenness(self):
    self.fetch_layer("greenness", defaults.NETASCORE_GREENNESS_QUERY)

  def fetch_water(self):
    self.fetch_layer("water", defaults.NETASCORE_WATER_QUERY)

  def add_layer_from_file(self, name, filepath, **kwargs):
    setattr(self, name, gpd.read_file(filepath, **kwargs))

  def add_noise(self, filepath, **kwargs):
    self.add_layer_from_file("noise", filepath, **kwargs)

  def write_elevation(self, filepath, inplace = True, **kwargs):
    elevated = ox.elevation.add_node_elevations_raster(self, filepath, **kwargs)
    graded = ox.elevation.add_edge_grades(elevated, add_absolute = False)
    if inplace:
      self.nodes = graded.nodes
      self.edges = graded.edges
    else:
      return graded

  def assess(self, assessor, inplace = True, **config):
    network = self if inplace else copy.deepcopy(self)
    metadata = assessor.run(network, **config)
    if config.get("write", True):
      name_fw = metadata["name"]["forward"]
      name_bw = metadata["name"]["backward"]
      logger.info(f"Wrote index to columns '{name_fw}' and '{name_bw}'")
    if not inplace:
      return network

  def clean(self, assessor, inplace = True, **config):
    network = self if inplace else copy.deepcopy(self)
    assessor.clean(network, **config)
    if not inplace:
      return network

  def _get_edge_attributes(self, *attrs):
    if self.is_multigraph():
      E = self.edges(keys = True, data = True)
    else:
      E = self.edges(data = True)
    # Subset edge data to contain only the specified attributes.
    E = [[e[:-1], {k:e[-1][k] for k in set(attrs) & set(e[-1].keys())}] for e in E]
    keys, data = zip(*E)
    out = pd.DataFrame(data, index = keys)
    return out.reindex(columns = attrs)

  def _get_edge_geometries(self, projected = False):
    geoms = ox.graph_to_gdfs(self, nodes = False, edges = True)["geometry"]
    if projected:
      geoms = geoms.to_crs(self.projected_crs)
    return geoms

  def _get_node_attributes(self, *attrs):
    N = self.nodes(data = True)
    # Subset node data to contain only the specified attributes.
    N = [[n[:-1], {k:n[-1][k] for k in set(attrs) & set(n[-1].keys())}] for n in N]
    keys, data = zip(*N)
    out = pd.DataFrame(data, index = keys)
    return out.reindex(columns = attrs)

  def _get_node_geometries(self, projected = False):
    geoms = ox.graph_to_gdfs(self, nodes = True, edges = False)["geometry"]
    if projected:
      geoms = geoms.to_crs(self.projected_crs)
    return geoms

  def _get_layer_attributes(self, *attrs):
    return getattr(self, layer)[attrs]

  def _get_layer_geometries(self, layer, projected = False):
    geoms = getattr(self, layer)["geometry"]
    if projected:
      geoms = geoms.to_crs(self.projected_crs)
    return geoms

  def _check_layer_presence(self, name, fetch = False):
    if getattr(self, name) is None:
      if fetch:
        try:
          getattr(self, f"fetch_{layer}")()
        except AttributeError:
          pass
        self._check_layer_presence(name, fetch = False)
      else:
        raise NetapyNetworkError(f"Network layer '{name}' is required but not present")