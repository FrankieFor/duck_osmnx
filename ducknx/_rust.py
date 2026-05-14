"""Optional Rust acceleration shim.

The Rust extension is published as the separate ``ducknx-core`` package
and pulled in via ``pip install ducknx[fast]``. When unavailable, the
pure-Python implementations remain in use unchanged.

Set ``DUCKNX_DEBUG_NO_RUST=1`` to force the Python fallback even when
the extension is installed (used by the equivalence test sweep).
"""

from __future__ import annotations

import os
import warnings

# Compatible ducknx_core version range. Bump this whenever the FFI signature
# changes; the wrapper refuses to use a mismatched extension rather than
# crash mysteriously at call time.
_COMPATIBLE = ">=0.1.0,<0.2.0"

HAVE_RUST = False
_simplify_topology = None
_cluster_assign = None

if not os.environ.get("DUCKNX_DEBUG_NO_RUST"):
    try:
        import ducknx_core  # type: ignore[import-not-found]
        from packaging.specifiers import SpecifierSet
        from packaging.version import Version

        if Version(ducknx_core.__version__) in SpecifierSet(_COMPATIBLE):
            _simplify_topology = ducknx_core.simplify_topology
            _cluster_assign = ducknx_core.cluster_assign
            HAVE_RUST = True
        else:
            warnings.warn(
                f"ducknx_core {ducknx_core.__version__} is outside compatible "
                f"range {_COMPATIBLE}; falling back to Python implementation.",
                stacklevel=2,
            )
    except ImportError:  # pragma: no cover - exercised on default install
        pass

__all__ = ["HAVE_RUST", "_cluster_assign", "_simplify_topology"]
