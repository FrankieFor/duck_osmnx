//! ducknx_core — native Rust acceleration for ducknx simplification.
//!
//! This extension exposes two hot kernels used by `ducknx.simplification`:
//!
//! * [`topology::simplify_topology`] — CSR DFS that mirrors the Python
//!   `_trace_paths` helper. Emits flat `(offsets, nodes_flat)` arrays.
//! * [`cluster::cluster_assign`] — point-in-polygon batch assignment used
//!   by `consolidate_intersections` after node-buffer dissolution.
//!
//! The Python side reaches these via the `ducknx._rust` shim which sets
//! `HAVE_RUST = True` when this extension is importable AND its version
//! falls inside the compatible specifier.

use pyo3::prelude::*;

pub mod cluster;
pub mod topology;

#[pymodule]
fn ducknx_core(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(topology::simplify_topology, m)?)?;
    m.add_function(wrap_pyfunction!(cluster::cluster_assign, m)?)?;
    Ok(())
}
