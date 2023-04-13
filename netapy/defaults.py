from netapy.profiles import NetascoreProfile

NETASCORE_PROFILES = {
  "bike": NetascoreProfile({
    "version": 1.1,
    "weights": {
      "bicycle_infrastructure": 0.2,
      "pedestrian_infrastructure": None,
      "designated_route": 0.1,
      "road_category": 0.3,
      "max_speed": 0.1,
      "max_speed_greatest": None,
      "parking": 0.1,
      "pavement": 0.1,
      "width": None,
      "gradient": 0.1,
      "number_lanes": None,
      "facilities": None,
      "crossings": None,
      "buildings": None,
      "greenness": None,
      "water": None,
      "noise": None
    },
    "overrides": [
      {
        "description": "combination of gradient and pavement (steep and loose/rough)",
        "indicator": "pavement",
        "output": {
          "type": "weight",
          "for": [
            "pavement",
            "gradient"
          ]
        },
        "mapping": {
          "{gravel, soft, cobble}": {
            "indicator": "gradient",
            "mapping": {
              "{-4, -3, 3, 4}": 1.6
            }
          }
        }
      }
    ],
    "indicator_mapping": [
      {
        "indicator": "bicycle_infrastructure",
        "mapping": {
          "bicycle_way": 1,
          "mixed_way": 0.9,
          "bicycle_lane": 0.75,
          "bus_lane": 0.75,
          "shared_lane": 0.5,
          "undefined": 0.2,
          "no": 0
        }
      },
      {
        "indicator": "designated_route",
        "mapping": {
          "international": 1,
          "national": 0.9,
          "regional": 0.85,
          "local": 0.8,
          "unknown": 0.8,
          "no": 0
        }
      },
      {
        "indicator": "road_category",
        "mapping": {
          "primary": 0,
          "secondary": 0.2,
          "residential": 0.8,
          "service": 0.85,
          "calmed": 0.9,
          "no_mit": 1,
          "path": 0
        }
      },
      {
        "indicator": "max_speed",
        "classes": {
          "ge100": 0,
          "ge80": 0.2,
          "ge70": 0.3,
          "ge60": 0.4,
          "ge50": 0.6,
          "ge30": 0.85,
          "g0": 0.9,
          "e0": 1
        }
      },
      {
        "indicator": "max_speed_greatest",
        "classes": {
          "ge100": 0,
          "ge80": 0.2,
          "ge70": 0.3,
          "ge60": 0.4,
          "ge50": 0.6,
          "ge30": 0.85,
          "g0": 0.9,
          "e0": 1
        }
      },
      {
        "indicator": "parking",
        "mapping": {
          "yes": 0,
          "no": 1,
          True: 0,
          False: 1
        }
      },
      {
        "indicator": "pavement",
        "mapping": {
          "asphalt": 1,
          "gravel": 0.75,
          "soft": 0.4,
          "cobble": 0
        }
      },
      {
        "indicator": "width",
        "classes": {
          "g5": 1,
          "g4": 0.9,
          "g3": 0.85,
          "g2": 0.5,
          "ge0": 0
        }
      },
      {
        "indicator": "gradient",
        "mapping": {
          "4": 0,
          "3": 0.25,
          "2": 0.4,
          "1": 0.5,
          "0": 0.9,
          "-1": 1,
          "-2": 0.95,
          "-3": 0.35,
          "-4": 0
        }
      },
      {
        "indicator": "number_lanes",
        "classes": {
          "g4": 0,
          "g3": 0.1,
          "g2": 0.2,
          "g1": 0.5,
          "ge0": 1
        }
      },
      {
        "indicator": "facilities",
        "classes": {
          "g0": 1,
          "e0": 0
        }
      },
      {
        "indicator": "buildings",
        "classes": {
          "ge80": 0,
          "g60": 0.2,
          "g40": 0.4,
          "g20": 0.6,
          "g0": 0.8,
          "e0": 1
        }
      },
      {
        "indicator": "greenness",
        "classes": {
          "g75": 1,
          "g50": 0.9,
          "g25": 0.8,
          "g0": 0.7,
          "e0": 0
        }
      },
      {
        "indicator": "water",
        "mapping": {
          True: 1,
          False: 0
        }
      },
      {
        "indicator": "noise",
        "classes": {
          "g70": 0,
          "g55": 0.6,
          "g10": 0.8,
          "ge0": 1
        }
      }
    ]
  }, name = "bike"),
  "walk": NetascoreProfile({
    "version": 1.1,
    "weights": {
      "bicycle_infrastructure": None,
      "pedestrian_infrastructure": 0.4,
      "designated_route": None,
      "road_category": 0.3,
      "max_speed": None,
      "max_speed_greatest": 0.3,
      "parking": None,
      "pavement": None,
      "width": None,
      "gradient": 0.3,
      "number_lanes": 0.1,
      "facilities": 0.3,
      "crossings": 0.2,
      "buildings": 0.1,
      "greenness": 0.3,
      "water": 0.4,
      "noise": 0.3
    },
    "overrides": [
      {
        "description": "fixed index value for sidewalk on primary/secondary roads (pedestrian_infrastructure/road_category)",
        "indicator": "pedestrian_infrastructure",
        "output": {
          "type": "index"
        },
        "mapping": {
          "sidewalk": {
            "indicator": "road_category",
            "mapping": {
              "{'secondary', 'primary'}": 0.2
            }
          }
        }
      }
    ],
    "indicator_mapping": [
      {
        "indicator": "pedestrian_infrastructure",
        "mapping": {
          "pedestrian_area": 1,
          "pedestrian_way": 1,
          "mixed_way": 0.85,
          "stairs": 0.7,
          "sidewalk": 0.5,
          "no": 0
        }
      },
      {
        "indicator": "road_category",
        "mapping": {
          "primary": 0,
          "secondary": 0.2,
          "residential": 0.8,
          "service": 0.85,
          "calmed": 0.9,
          "no_mit": 1,
          "path": 1
        }
      },
      {
        "indicator": "max_speed",
        "classes": {
          "ge100": 0,
          "ge80": 0.2,
          "ge70": 0.3,
          "ge60": 0.4,
          "ge50": 0.6,
          "ge30": 0.85,
          "g0": 0.9,
          "e0": 1
        }
      },
      {
        "indicator": "max_speed_greatest",
        "classes": {
          "ge100": 0,
          "ge80": 0.2,
          "ge70": 0.3,
          "ge60": 0.4,
          "ge50": 0.6,
          "ge30": 0.85,
          "g0": 0.9,
          "e0": 1
        }
      },
      {
        "indicator": "parking",
        "mapping": {
          "yes": 0,
          "no": 1,
          True: 0,
          False: 1
        }
      },
      {
        "indicator": "pavement",
        "mapping": {
          "asphalt": 1,
          "gravel": 0.75,
          "soft": 0.4,
          "cobble": 0
        }
      },
      {
        "indicator": "width",
        "classes": {
          "g5": 1,
          "g4": 0.9,
          "g3": 0.85,
          "g2": 0.5,
          "ge0": 0
        }
      },
      {
        "indicator": "gradient",
        "mapping": {
          "4": 0.25,
          "3": 0.5,
          "2": 0.7,
          "1": 1,
          "0": 1,
          "-1": 1,
          "-2": 0.7,
          "-3": 0.5,
          "-4": 0.25
        }
      },
      {
        "indicator": "number_lanes",
        "classes": {
          "g4": 0,
          "g3": 0.1,
          "g2": 0.2,
          "g1": 0.5,
          "ge0": 1
        }
      },
      {
        "indicator": "facilities",
        "classes": {
          "g0": 1,
          "e0": 0
        }
      },
      {
        "indicator": "crossings",
        "classes": {
          "e0": {
            "indicator": "road_category",
            "mapping": {
              "primary": 0,
              "secondary": 0,
              None: 0,
              "residential": 0.5,
              "_default_": 1
            }
          },
          "g0": 1
        }
      },
      {
        "indicator": "buildings",
        "classes": {
          "ge80": 0,
          "g60": 0.2,
          "g40": 0.4,
          "g20": 0.6,
          "g0": 0.8,
          "e0": 1
        }
      },
      {
        "indicator": "greenness",
        "classes": {
          "g75": 1,
          "g50": 0.9,
          "g25": 0.8,
          "g0": 0.7,
          "e0": 0
        }
      },
      {
        "indicator": "water",
        "mapping": {
          True: 1,
          False: 0
        }
      },
      {
        "indicator": "noise",
        "classes": {
          "g70": 0,
          "g55": 0.6,
          "g10": 0.8,
          "ge0": 1
        }
      }
    ]
  }, name = "walk")
}

NETASCORE_NAMING_CONFIG = {
  "index_prefix": "index_",
  "index_suffix": "",
  "attribute_prefix": "",
  "attribute_suffix": "",
  "forward_prefix": "",
  "forward_suffix": ":forward",
  "backward_prefix": "",
  "backward_suffix": ":backward"
}

NETASCORE_STREET_KEYS = [
  "name",
  "highway",
  "access",
  "oneway",
  "bridge",
  "tunnel",
  "junction",
  "service",
  "maxspeed",
  "lanes",
  "width",
  "est_width",
  "surface",
  "tracktype",
  "smoothness",
  "bicycle",
  "cyclestreet",
  "bicycle_road",
  "cycleway",
  "cycleway:both",
  "cycleway:left",
  "cycleway:right"
  "foot",
  "footway",
  "sidewalk",
  "sidewalk:both",
  "sidewalk:left",
  "sidewalk:right",
  "segregated"
]

NETASCORE_BUILDINGS_QUERY = {
  "building": True
}

NETASCORE_CROSSINGS_QUERY = {
  "highway": "crossing"
}

NETASCORE_FACILITIES_QUERY = {
  "amenity": [
    "arts_centre",
    "artwork",
    "attraction",
    "bar",
    "biergarten",
    "cafe",
    "castle",
    "cinema",
    "museum",
    "park",
    "pub",
    "restaurant",
    "swimming_pool",
    "theatre",
    "viewpoint",
    "bakery",
    "beverages",
    "butcher",
    "clothes",
    "department_store",
    "fast_food",
    "florist",
    "furniture_shop",
    "kiosk",
    "mall",
    "outdoor_shop",
    "pharmacy",
    "shoe_shop",
    "sports_shop",
    "supermarket",
    "commercial",
    "retail",
    "shop",
    "university",
    "school",
    "college",
    "gymnasium",
    "kindergarten",
    "boarding_school",
    "music_school",
    "riding_school",
    "school;dormitory"
  ],
  "tourism": [
    "museum",
    "attraction",
    "gallery",
    "viewpoint",
    "zoo"
  ]
}

NETASCORE_GREENNESS_QUERY = {
  "landuse": [
    "forest",
    "grass",
    "meadow",
    "village_green",
    "recreation_ground",
    "vineyard",
    "flowerbed",
    "farmland",
    "heath",
    "nature_reseve",
    "park",
    "greenfield"
  ],
  "leisure": [
    "garden",
    "golf_course",
    "park"
  ],
  "natural": [
    "tree",
    "wood",
    "grassland",
    "heath",
    "scrub"
  ]
}

NETASCORE_WATER_QUERY = {
  "waterway": True,
  "natural": "water",
  "tunnel": False
}