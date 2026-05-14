"""Profile simplify+build on Berlin large bbox; commit results as artifact."""

from __future__ import annotations

import cProfile
import io
import pstats
import tracemalloc
from pathlib import Path

from ducknx import _pbf_reader
from ducknx import graph
from ducknx import settings
from ducknx import simplification
from ducknx import utils_geo

PBF = Path("berlin-latest.osm.pbf")
BBOX = (13.1, 52.3, 13.8, 52.7)


def main() -> None:
    """Run profiler over build + simplify and dump cumulative stats."""
    settings.pbf_file_path = str(PBF)
    settings.log_console = False
    polygon = utils_geo.bbox_to_poly(BBOX)
    nodes_df, ways_df = _pbf_reader._read_pbf_network_duckdb(polygon, "drive", None, PBF)

    tracemalloc.start()
    pr = cProfile.Profile()
    pr.enable()
    G = graph._create_graph_from_dfs(nodes_df, ways_df, False)
    simplification.simplify_graph(G)
    pr.disable()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    buf = io.StringIO()
    pstats.Stats(pr, stream=buf).strip_dirs().sort_stats("cumulative").print_stats(40)
    out = Path("benchmarks/profiles/phaseA-baseline.txt")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(f"tracemalloc peak: {peak / 1024 / 1024:.1f} MB\n\n" + buf.getvalue())
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
