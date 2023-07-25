# netapy: A Python library to assess street network suitability for sustainable transport modes

The netapy package is a pure-python implementation of the [NetAScore Toolbox](https://github.com/plus-mobilitylab/netascore), developed by the University of Salzburg. It is coded in such a way that it should make it easy to adapt or extend the default workflow, and to add other approaches to street network suitability assessment for sustainable transport modes as well.

This version of netapy is still work in progress, and does not yet implement all parts of the netascore workflow. Currently it is lacking 1) derivation of some of the relevant attributes from OpenStreetMap data (they are assigned randomly instead), 2) the ability to parse overrides defintions of mode profiles, and 3) the ability to add non-OpenStreetMap attributes to the network (such as noise).

## Installation

Clone the repository with:

```bash
git clone git@github.com:plus-mobilitylab/netapy.git
cd netapy
```

Install the package as:

```bash
pip install .
pip install -e .  # Install in editable mode
```

## Usage

Basic usage example:

```python
import netapy

network = netapy.networks.NetascoreNetwork.from_place("Anif")
assessor = netapy.assessors.NetascoreAssessor(profile = "bike")

network.assess(assessor)
```

See also the [demo notebook](demo/demo.ipynb)

## License

This project is licensed under the MIT license. For details please see [LICENSE](LICENSE).