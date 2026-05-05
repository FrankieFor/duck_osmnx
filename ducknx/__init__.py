# ruff: noqa: D205  # numpydoc ignore=SS06
"""
ducknx is a Python package to easily download, model, analyze, and visualize
street networks and other geospatial features from OpenStreetMap.

Full documentation at: https://osmnx.readthedocs.io

If you use ducknx in your work, please cite: https://doi.org/10.1111/gean.70009
"""

from importlib.metadata import version as metadata_version

# expose the package version
__version__ = metadata_version("ducknx")

# expose the package's public modules
from . import _errors as _errors
from . import bearing as bearing
from . import convert as convert
from . import distance as distance
from . import elevation as elevation
from . import features as features
from . import geocoder as geocoder
from . import graph as graph
from . import io as io
from . import projection as projection
from . import routing as routing
from . import settings as settings
from . import simplification as simplification
from . import stats as stats
from . import truncate as truncate
from . import utils as utils
from . import utils_geo as utils_geo

# expose top-level shortcut names (plot.* loaded lazily via _api_v1.__getattr__)
from ._api_v1 import *  # noqa: F403


def __getattr__(name: str) -> object:
    """Lazy `dx.plot` access so importing ducknx does not require matplotlib."""
    if name == "plot":
        import importlib  # noqa: PLC0415

        module = importlib.import_module("ducknx.plot")
        globals()["plot"] = module
        return module
    from ._api_v1 import __getattr__ as _api_getattr  # noqa: PLC0415

    return _api_getattr(name)
