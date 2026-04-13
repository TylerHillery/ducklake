#!/usr/bin/env python3
"""
Generate ClickBench comparison charts from CSV results.

Produces:
  1. Heatmap table — benchmarks (x) vs queries (y), colored by execution time
  2. Horizontal bar charts — one per query, fastest first

Usage:
  uv run python clickbench/chart.py
  uv run python clickbench/chart.py --csv clickbench/results/clickbench_results.csv
  uv run python clickbench/chart.py --run hot       # hot (default), cold, or all
  uv run python clickbench/chart.py --top 15        # show only top N fastest systems
"""

import argparse
from pathlib import Path

import altair as alt
import pandas as pd

SCRIPT_DIR = Path(__file__).parent
DEFAULT_CSV = SCRIPT_DIR / "results" / "clickbench_results.csv"


def load(csv_path: Path, run: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["time_seconds"] = pd.to_numeric(df["time_seconds"], errors="coerce")

    if run == "hot":
        # Run 3 = hottest
        df = df[df["run_number"] == 3]
    elif run == "cold":
        # Run 1 = cold
        df = df[df["run_number"] == 1]
    else:
        # All runs — take the minimum across runs per query per benchmark
        df = df.groupby(["benchmark_id", "query_number"], as_index=False).agg(
            time_seconds=("time_seconds", "min"),
            timestamp=("timestamp", "first"),
            instance_size=("instance_size", "first"),
            vcpus=("vcpus", "first"),
            memory_gb=("memory_gb", "first"),
            table_name=("table_name", "first"),
            query_text=("query_text", "first"),
        )

    return df


def heatmap(df: pd.DataFrame, top_n: int | None) -> alt.Chart:
    """Heatmap: benchmarks on x-axis, queries on y-axis, color = log(time)."""
    # Rank systems by geometric mean of query times (lower = faster)
    import numpy as np
    geo_mean = (
        df.groupby("benchmark_id")["time_seconds"]
        .apply(lambda x: np.exp(np.log(x.clip(lower=0.001)).mean()))
        .sort_values()
    )

    if top_n:
        geo_mean = geo_mean.head(top_n)

    system_order = geo_mean.index.tolist()
    df = df[df["benchmark_id"].isin(system_order)]

    # Extract query number as int for sorting
    df = df.copy()
    df["q_num"] = df["query_number"].str.replace("Q", "").astype(int)
    query_order = [f"Q{i}" for i in sorted(df["q_num"].unique())]

    valid = df["time_seconds"].dropna()
    domain_min = max(valid.min(), 0.001)
    domain_max = valid.max()
    # Threshold for switching text color: geometric midpoint of the domain
    mid = (domain_min * domain_max) ** 0.5

    chart = (
        alt.Chart(df)
        .mark_rect()
        .encode(
            x=alt.X("benchmark_id:N", sort=system_order, title=None,
                     axis=alt.Axis(labelAngle=-45, labelLimit=200, labelFontSize=8)),
            y=alt.Y("query_number:N", sort=query_order, title="Query"),
            color=alt.Color(
                "time_seconds:Q",
                scale=alt.Scale(type="log", scheme="redyellowgreen", reverse=True,
                                domainMin=domain_min, domainMax=domain_max),
                title="Time (s)",
            ),
            tooltip=[
                alt.Tooltip("benchmark_id:N", title="System"),
                alt.Tooltip("query_number:N", title="Query"),
                alt.Tooltip("time_seconds:Q", title="Time (s)", format=".3f"),
            ],
        )
        .properties(width=max(len(system_order) * 55, 600), height=43 * 18, title="ClickBench Heatmap")
    )

    # Two text layers: white for dark cells (fast green + slow red), black for mid-range
    # Use two thresholds to catch both ends of the color scale
    low_thresh = domain_min * ((domain_max / domain_min) ** 0.2)   # darkest greens
    high_thresh = domain_min * ((domain_max / domain_min) ** 0.75) # oranges/reds

    text_white = (
        alt.Chart(df)
        .mark_text(fontSize=7, fontWeight="bold")
        .encode(
            x=alt.X("benchmark_id:N", sort=system_order),
            y=alt.Y("query_number:N", sort=query_order),
            text=alt.Text("time_seconds:Q", format=".2f"),
            color=alt.value("white"),
            opacity=alt.condition(
                (alt.datum.time_seconds <= low_thresh) | (alt.datum.time_seconds >= high_thresh),
                alt.value(1),
                alt.value(0),
            ),
        )
    )

    text_black = (
        alt.Chart(df)
        .mark_text(fontSize=7, fontWeight="bold")
        .encode(
            x=alt.X("benchmark_id:N", sort=system_order),
            y=alt.Y("query_number:N", sort=query_order),
            text=alt.Text("time_seconds:Q", format=".2f"),
            color=alt.value("black"),
            opacity=alt.condition(
                (alt.datum.time_seconds > low_thresh) & (alt.datum.time_seconds < high_thresh),
                alt.value(1),
                alt.value(0),
            ),
        )
    )

    return (chart + text_white + text_black).configure_axis(labelFontSize=10)


def bar_charts(df: pd.DataFrame, top_n: int | None) -> alt.Chart:
    """One horizontal bar chart per query, sorted by time (fastest first), one chart per row."""
    import numpy as np

    # Filter to top N by geometric mean if requested
    if top_n:
        geo_mean = (
            df.groupby("benchmark_id")["time_seconds"]
            .apply(lambda x: np.exp(np.log(x.clip(lower=0.001)).mean()))
            .sort_values()
        )
        system_order = geo_mean.head(top_n).index.tolist()
        df = df[df["benchmark_id"].isin(system_order)]

    df = df.copy()
    df["q_num"] = df["query_number"].str.replace("Q", "").astype(int)
    num_systems = df["benchmark_id"].nunique()

    # Build one chart per query so each has its own independent zoom
    charts = []
    for q in sorted(df["q_num"].unique()):
        qname = f"Q{q}"
        qdf = df[df["query_number"] == qname]

        zoom = alt.selection_interval(bind="scales", encodings=["x"], name=f"zoom_q{q}")

        c = (
            alt.Chart(qdf)
            .mark_bar()
            .encode(
                x=alt.X("time_seconds:Q", title="Time (s)"),
                y=alt.Y("benchmark_id:N",
                         sort=alt.EncodingSortField(field="time_seconds", order="ascending"),
                         title=None,
                         axis=alt.Axis(labelFontSize=8, labelLimit=300)),
                color=alt.Color(
                    "time_seconds:Q",
                    scale=alt.Scale(type="log", scheme="greens", reverse=True,
                                    domainMin=0.01, domainMax=300),
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("benchmark_id:N", title="System"),
                    alt.Tooltip("time_seconds:Q", title="Time (s)", format=".3f"),
                ],
            )
            .properties(width=500, height=max(num_systems * 16, 150), title=qname)
            .add_params(zoom)
        )
        charts.append(c)

    return alt.vconcat(*charts).resolve_scale(color="shared")


def main():
    parser = argparse.ArgumentParser(description="Generate ClickBench comparison charts")
    parser.add_argument("--csv", default=str(DEFAULT_CSV), help="Path to CSV results file")
    parser.add_argument("--run", default="hot", choices=["hot", "cold", "all"], help="Which run to plot (default: hot)")
    parser.add_argument("--top", type=int, default=None, help="Show only top N fastest systems")
    parser.add_argument("--output", default=str(SCRIPT_DIR / "results"), help="Output directory")
    args = parser.parse_args()

    df = load(Path(args.csv), args.run)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loaded {len(df)} rows, {df['benchmark_id'].nunique()} systems, {df['query_number'].nunique()} queries")
    print(f"Run: {args.run}, Top: {args.top or 'all'}")

    # Heatmap
    print("Generating heatmap…")
    h = heatmap(df, args.top)
    heatmap_path = out_dir / f"heatmap_{args.run}.html"
    h.save(str(heatmap_path))
    print(f"  → {heatmap_path}")

    # Bar charts
    print("Generating bar charts…")
    b = bar_charts(df, args.top)
    bars_path = out_dir / f"bars_{args.run}.html"
    b.save(str(bars_path))
    print(f"  → {bars_path}")


if __name__ == "__main__":
    main()
