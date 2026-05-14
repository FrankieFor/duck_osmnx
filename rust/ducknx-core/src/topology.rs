//! CSR DFS that mirrors `ducknx.simplification._trace_paths`.

use numpy::{PyArray1, PyReadonlyArray1, ToPyArray};
use pyo3::prelude::*;
use std::collections::HashSet;

/// Native CSR DFS that mirrors Python `_trace_paths`.
///
/// Walks each endpoint, picks the first non-visited successor, and follows
/// the chain until another endpoint is reached. Emits `(offsets, nodes_flat)`
/// where path `i` spans `nodes_flat[offsets[i]..offsets[i+1]]`.
pub fn trace_paths_native(
    succ_indptr: &[i64],
    succ_indices: &[i64],
    is_endpoint: &[bool],
) -> (Vec<i64>, Vec<i64>) {
    let mut offsets: Vec<i64> = vec![0];
    let mut nodes_flat: Vec<i64> = Vec::new();

    let n = is_endpoint.len();
    // Walk endpoints in ascending order to match the Python `sorted(...)`
    // iteration order; this keeps path ordering byte-identical with the
    // pure-Python fallback so equivalence tests pass.
    for ei in 0..n as i64 {
        if !is_endpoint[ei as usize] {
            continue;
        }
        let s = succ_indptr[ei as usize] as usize;
        let e = succ_indptr[ei as usize + 1] as usize;

        // Deduplicate parallel successors to match Python's `seen_succs` set.
        let mut seen_succs: HashSet<i64> = HashSet::new();
        for &succ_val in &succ_indices[s..e] {
            if !seen_succs.insert(succ_val) {
                continue;
            }
            let succ = succ_val;
            if is_endpoint[succ as usize] {
                continue;
            }

            let mut path: Vec<i64> = vec![ei, succ];
            let mut path_set: HashSet<i64> = HashSet::from([ei, succ]);

            // pick first non-visited successor of `succ`
            let ss = succ_indptr[succ as usize] as usize;
            let se = succ_indptr[succ as usize + 1] as usize;
            let picked = succ_indices[ss..se]
                .iter()
                .copied()
                .find(|n| !path_set.contains(n));
            if picked.is_none() {
                nodes_flat.extend_from_slice(&path);
                offsets.push(nodes_flat.len() as i64);
                continue;
            }
            let mut current = picked.unwrap();
            path.push(current);
            path_set.insert(current);

            while !is_endpoint[current as usize] {
                let cs = succ_indptr[current as usize] as usize;
                let ce = succ_indptr[current as usize + 1] as usize;
                let onward: Vec<i64> = succ_indices[cs..ce]
                    .iter()
                    .copied()
                    .filter(|n| !path_set.contains(n))
                    .collect();
                if onward.len() == 1 {
                    current = onward[0];
                    path.push(current);
                    path_set.insert(current);
                } else if onward.is_empty() {
                    // self-loop closing back to ei?
                    if succ_indices[cs..ce].contains(&ei) {
                        path.push(ei);
                    }
                    break;
                } else {
                    // unexpected branch; abort path
                    break;
                }
            }

            nodes_flat.extend_from_slice(&path);
            offsets.push(nodes_flat.len() as i64);
        }
    }
    (offsets, nodes_flat)
}

/// PyO3 wrapper around [`trace_paths_native`].
///
/// Accepts CSR successor arrays and a uint8 endpoint mask (1 = endpoint).
/// Returns `(offsets, nodes_flat)` as int64 numpy arrays. Releases the GIL
/// during the DFS so callers running this from a Python thread pool aren't
/// blocked.
#[pyfunction]
pub fn simplify_topology<'py>(
    py: Python<'py>,
    succ_indptr: PyReadonlyArray1<'py, i64>,
    succ_indices: PyReadonlyArray1<'py, i64>,
    is_endpoint: PyReadonlyArray1<'py, u8>,
) -> PyResult<(Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<i64>>)> {
    let succ_indptr_slice = succ_indptr.as_slice()?;
    let succ_indices_slice = succ_indices.as_slice()?;
    let endpoint_bools: Vec<bool> = is_endpoint.as_slice()?.iter().map(|b| *b != 0).collect();

    // Release the GIL during the DFS — for ~1M-node graphs this can run
    // for seconds and we shouldn't block other Python threads.
    let (offsets, nodes_flat) = py.allow_threads(|| {
        trace_paths_native(succ_indptr_slice, succ_indices_slice, &endpoint_bools)
    });
    Ok((offsets.to_pyarray_bound(py), nodes_flat.to_pyarray_bound(py)))
}
