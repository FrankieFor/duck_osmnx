//! Point-in-polygon batch assignment for `consolidate_intersections`.
//!
//! Takes shapely-emitted WKB cluster polygons + node `(x, y)` arrays and
//! returns a cluster index per node, or `-1` if the node falls outside every
//! cluster polygon (the Python caller then runs a nearest-cluster fallback
//! to match legacy semantics).

use geo::{BoundingRect, Contains, Geometry, Point};
use numpy::{PyArray1, PyReadonlyArray1, ToPyArray};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rstar::RTree;

struct PolyEntry {
    geom: Geometry<f64>,
    idx: i64,
    aabb: rstar::AABB<[f64; 2]>,
}

impl rstar::RTreeObject for PolyEntry {
    type Envelope = rstar::AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        self.aabb
    }
}

fn aabb_for(geom: &Geometry<f64>) -> Option<rstar::AABB<[f64; 2]>> {
    let bbox = match geom {
        Geometry::Polygon(p) => p.bounding_rect()?,
        Geometry::MultiPolygon(mp) => mp.bounding_rect()?,
        _ => return None,
    };
    Some(rstar::AABB::from_corners(
        [bbox.min().x, bbox.min().y],
        [bbox.max().x, bbox.max().y],
    ))
}

/// Parse a shapely-emitted WKB buffer into a [`geo::Geometry`].
///
/// Shapely emits ISO WKB by default. The `wkb` crate's `reader` module
/// exposes `read_wkb` which auto-detects endianness and supports Polygon /
/// MultiPolygon (including those with holes), which are the only shapes
/// produced by `_merge_nodes_geometric`.
fn parse_wkb(bytes: &[u8]) -> Result<Geometry<f64>, String> {
    let mut cursor = std::io::Cursor::new(bytes);
    wkb::reader::read_wkb(&mut cursor).map_err(|e| format!("{e:?}"))
}

#[pyfunction]
pub fn cluster_assign<'py>(
    py: Python<'py>,
    node_xs: PyReadonlyArray1<'py, f64>,
    node_ys: PyReadonlyArray1<'py, f64>,
    cluster_polygons_wkb: Vec<Bound<'py, PyBytes>>,
) -> PyResult<Bound<'py, PyArray1<i64>>> {
    let xs = node_xs.as_slice()?;
    let ys = node_ys.as_slice()?;

    let mut entries: Vec<PolyEntry> = Vec::with_capacity(cluster_polygons_wkb.len());
    for (i, wkb_bytes) in cluster_polygons_wkb.iter().enumerate() {
        let bytes = wkb_bytes.as_bytes();
        let geom = parse_wkb(bytes)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("WKB: {e}")))?;
        let aabb = aabb_for(&geom).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "cluster polygons must be Polygon or MultiPolygon with a bounding rect",
            )
        })?;
        entries.push(PolyEntry {
            geom,
            idx: i as i64,
            aabb,
        });
    }
    let tree = RTree::bulk_load(entries);

    let mut out = vec![-1i64; xs.len()];
    py.allow_threads(|| {
        for i in 0..xs.len() {
            let pt = Point::new(xs[i], ys[i]);
            let point_aabb = rstar::AABB::from_point([xs[i], ys[i]]);
            for cand in tree.locate_in_envelope_intersecting(&point_aabb) {
                let contains = match &cand.geom {
                    Geometry::Polygon(p) => p.contains(&pt),
                    Geometry::MultiPolygon(mp) => mp.contains(&pt),
                    _ => false,
                };
                if contains {
                    out[i] = cand.idx;
                    break;
                }
            }
        }
    });
    Ok(out.to_pyarray_bound(py))
}
