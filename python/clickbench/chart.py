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


def load(csv_path: Path, run: str, providers: list[str] | None = None) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["time_seconds"] = pd.to_numeric(df["time_seconds"], errors="coerce")

    if providers:
        if "provider" not in df.columns:
            print("Warning: CSV has no 'provider' column — re-run convert_clickbench.py and benchmark.py")
        else:
            df = df[df["provider"].isin(providers)]

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


def speedup_chart(df: pd.DataFrame, bid1: str, bid2: str) -> alt.Chart:
    """
    Head-to-head diverging bar chart.

    X axis: log₂(time_bid2 / time_bid1)
      > 0  → bid1 is faster (bar goes right, green)
      < 0  → bid2 is faster (bar goes left, red)
      ticks labeled as ×2, ×4, ×8 … on each side

    Y axis: query number, sorted Q0–Q42.
    """
    import numpy as np

    d1 = (
        df[df["benchmark_id"] == bid1][["query_number", "time_seconds"]]
        .rename(columns={"time_seconds": "t1"})
    )
    d2 = (
        df[df["benchmark_id"] == bid2][["query_number", "time_seconds"]]
        .rename(columns={"time_seconds": "t2"})
    )
    merged = d1.merge(d2, on="query_number").dropna(subset=["t1", "t2"])
    merged = merged[merged["t1"] > 0]

    merged["log2_speedup"] = np.log2(merged["t2"] / merged["t1"])
    merged["ratio"] = merged["t2"] / merged["t1"]
    merged["label"] = merged["ratio"].apply(
        lambda r: f"{r:.2f}× faster" if r >= 1 else f"{1/r:.2f}× faster"
    )
    merged["faster"] = merged["log2_speedup"].apply(
        lambda x: bid1 if x > 0 else (bid2 if x < 0 else "equal")
    )
    merged["q_num"] = merged["query_number"].str.replace("Q", "").astype(int)
    query_order = [f"Q{i}" for i in sorted(merged["q_num"].unique())]

    tick_expr = (
        "datum.value == 0 ? '1×' : "
        "datum.value > 0 ? format(pow(2, datum.value), '.2~r') + '×' : "
        "'-' + format(pow(2, -datum.value), '.2~r') + '×'"
    )

    bars = (
        alt.Chart(merged)
        .mark_bar()
        .encode(
            x=alt.X(
                "log2_speedup:Q",
                title=f"← {bid2} faster  |  {bid1} faster →",
                axis=alt.Axis(labelExpr=tick_expr, grid=True),
            ),
            y=alt.Y("query_number:N", sort=query_order, title="Query",
                    axis=alt.Axis(labelFontSize=9)),
            color=alt.condition(
                alt.datum.log2_speedup >= 0,
                alt.value("#22c55e"),
                alt.value("#ef4444"),
            ),
            tooltip=[
                alt.Tooltip("query_number:N", title="Query"),
                alt.Tooltip("t1:Q", title=f"{bid1} (s)", format=".3f"),
                alt.Tooltip("t2:Q", title=f"{bid2} (s)", format=".3f"),
                alt.Tooltip("label:N", title="Speedup"),
                alt.Tooltip("faster:N", title="Faster system"),
            ],
        )
        .properties(
            width=600,
            height=max(len(query_order) * 14, 400),
            title=f"{bid1}  vs  {bid2}",
        )
    )

    rule = (
        alt.Chart(pd.DataFrame({"x": [0]}))
        .mark_rule(color="black", strokeWidth=1)
        .encode(x=alt.X("x:Q"))
    )

    return (bars + rule).configure_axis(labelFontSize=10)


def main():
    parser = argparse.ArgumentParser(description="Generate ClickBench comparison charts")
    parser.add_argument("--csv", default=str(DEFAULT_CSV), help="Path to CSV results file")
    parser.add_argument("--run", default="hot", choices=["hot", "cold", "all"], help="Which run to plot (default: hot)")
    parser.add_argument("--top", type=int, default=None, help="Show only top N fastest systems")
    parser.add_argument("--provider", nargs="+", default=None, metavar="PROVIDER",
                        help="Filter to one or more providers (e.g. --provider supabase_ducklake pg_duckdb)")
    parser.add_argument("--compare", nargs=2, metavar="BENCHMARK_ID",
                        help="Head-to-head speedup chart for two benchmark IDs")
    parser.add_argument("--output", default=str(SCRIPT_DIR / "results"), help="Output directory")
    args = parser.parse_args()

    df = load(Path(args.csv), args.run, args.provider)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loaded {len(df)} rows, {df['benchmark_id'].nunique()} systems, {df['query_number'].nunique()} queries")
    print(f"Run: {args.run}, Top: {args.top or 'all'}")

    if args.compare:
        bid1, bid2 = args.compare
        print(f"Generating head-to-head: {bid1}  vs  {bid2}…")
        c = speedup_chart(df, bid1, bid2)
        out = out_dir / f"headtohead_{args.run}.html"
        c.save(str(out))
        print(f"  → {out}")
        return

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
