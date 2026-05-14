//! Unit tests for the topology kernel mirror the Python equivalence harness.

use ducknx_core::topology::trace_paths_native;

#[test]
fn straight_chain_collapses() {
    // 0 -> 1 -> 2 -> 3, endpoints are {0, 3}, expect one path [0,1,2,3]
    let succ_indptr = vec![0i64, 1, 2, 3, 3];
    let succ_indices = vec![1i64, 2, 3];
    let endpoints = vec![true, false, false, true];
    let (offsets, nodes_flat) = trace_paths_native(&succ_indptr, &succ_indices, &endpoints);
    assert_eq!(offsets, vec![0, 4]);
    assert_eq!(nodes_flat, vec![0, 1, 2, 3]);
}

#[test]
fn branch_makes_branch_endpoint_only_traverse_each_arm_once() {
    // 0 -> 1, 0 -> 3, 1 -> 2, endpoints {0, 2, 3}
    // succ rows: 0 -> [1,3]; 1 -> [2]; 2 -> []; 3 -> []
    let succ_indptr = vec![0i64, 2, 3, 3, 3];
    let succ_indices = vec![1i64, 3, 2];
    let endpoints = vec![true, false, true, true];
    let (offsets, _) = trace_paths_native(&succ_indptr, &succ_indices, &endpoints);
    // From endpoint 0 we start two paths (one toward 1->2, one toward 3
    // which is itself an endpoint so skipped). From endpoint 2/3 there is
    // no outgoing edge. So exactly one path is emitted.
    assert_eq!(offsets.len() - 1, 1);
}
