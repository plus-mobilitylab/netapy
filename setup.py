from setuptools import setup
from pathlib import Path

here = Path(__file__).absolute().parent

# Version number
with open("netapy/__init__.py") as f:
  for line in f:
    if "__version__" in line:
      version = line.split("=")[1].strip().strip('"').strip('"')
      continue

# The text of the README file.
try:
  with open(here / "README.md") as f:
    README = f.read()
except FileNotFoundError:
  README = ""

# Requirements.
try:
  with open((here / "requirements.txt"), encoding = "utf-8") as f:
    requirements = f.readlines()
  requirements = [line.strip() for line in requirements]
except FileNotFoundError:
  requirements = []

setup(
  name = "netapy",
  version = version,
  url = "https://github.com/plus-mobilitylab/netapy",
  author = "Lucas van der Meer",
  author_email = "lucas.vandermeer@plus.ac.at",
  description = "Assess street network suitability for sustainable transport modes",
  long_description = README,
  long_description_content_type = "text/markdown",
  license = "MIT",
  classifiers = [
    # As from http://pypi.python.org/pypi?%3Aaction = list_classifiers
    # "Development Status :: 1 - Planning",
    "Development Status :: 2 - Pre-Alpha",
    # "Development Status :: 3 - Alpha",
    # "Development Status :: 4 - Beta",
    # "Development Status :: 5 - Production/Stable",
    # "Development Status :: 6 - Mature",
    # "Development Status :: 7 - Inactive",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Topic :: Scientific/Engineering :: Information Analysis",
    "Topic :: Scientific/Engineering :: GIS",
    "Topic :: Software Development :: Libraries :: Python Modules"
  ],
  packages = ["netapy"],
  platforms = "any",
  install_requires = requirements
)