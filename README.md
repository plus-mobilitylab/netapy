# netapy: A Python library to assess street network suitability for sustainable transport modes

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