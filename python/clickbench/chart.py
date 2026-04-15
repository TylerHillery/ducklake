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
    df["run_number"] = pd.to_numeric(df["run_number"], errors="coerce")

    if providers:
        if "provider" not in df.columns:
            print(
                "Warning: CSV has no 'provider' column — re-run convert_clickbench.py and benchmark.py"
            )
        else:
            df = df[df["provider"].isin(providers)]

    if run == "hot":
        df = df[df["run_number"].isin([2, 3])]
    elif run == "cold":
        df = df[df["run_number"] == 1]

    # Deduplicate: keep min time per (benchmark_id, query_number) in case of repeated runs
    df = (
        df.sort_values("time_seconds")
        .groupby(["benchmark_id", "query_number"], as_index=False)
        .first()
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

    chart = (
        alt.Chart(df)
        .mark_rect()
        .encode(
            x=alt.X(
                "benchmark_id:N",
                sort=system_order,
                title=None,
                axis=alt.Axis(labelAngle=-45, labelLimit=200, labelFontSize=8),
            ),
            y=alt.Y("query_number:N", sort=query_order, title="Query"),
            color=alt.Color(
                "time_seconds:Q",
                scale=alt.Scale(
                    type="log",
                    scheme="redyellowgreen",
                    reverse=True,
                    domainMin=domain_min,
                    domainMax=domain_max,
                ),
                title="Time (s)",
            ),
            tooltip=[
                alt.Tooltip("benchmark_id:N", title="System"),
                alt.Tooltip("query_number:N", title="Query"),
                alt.Tooltip("time_seconds:Q", title="Time (s)", format=".3f"),
            ],
        )
        .properties(
            width=max(len(system_order) * 55, 600),
            height=43 * 18,
            title="ClickBench Heatmap",
        )
    )

    # Two text layers: white for dark cells (fast green + slow red), black for mid-range
    # Use two thresholds to catch both ends of the color scale
    low_thresh = domain_min * ((domain_max / domain_min) ** 0.2)  # darkest greens
    high_thresh = domain_min * ((domain_max / domain_min) ** 0.75)  # oranges/reds

    text_white = (
        alt.Chart(df)
        .mark_text(fontSize=7, fontWeight="bold")
        .encode(
            x=alt.X("benchmark_id:N", sort=system_order),
            y=alt.Y("query_number:N", sort=query_order),
            text=alt.Text("time_seconds:Q", format=".2f"),
            color=alt.value("white"),
            opacity=alt.condition(
                (alt.datum.time_seconds <= low_thresh)
                | (alt.datum.time_seconds >= high_thresh),
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
                (alt.datum.time_seconds > low_thresh)
                & (alt.datum.time_seconds < high_thresh),
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

    # Total time across all queries per system.
    totals_df = (
        df.groupby("benchmark_id", as_index=False)["time_seconds"]
        .sum()
        .rename(columns={"time_seconds": "total_time_seconds"})
    )
    total_min = max(totals_df["total_time_seconds"].min(), 0.001)
    total_max = totals_df["total_time_seconds"].max()
    if total_max <= total_min:
        total_max = total_min * 1.01

    total_zoom = alt.selection_interval(
        bind="scales", encodings=["x"], name="zoom_total"
    )
    total_chart = (
        alt.Chart(totals_df)
        .mark_bar()
        .encode(
            x=alt.X("total_time_seconds:Q", title="Total Time (s)"),
            y=alt.Y(
                "benchmark_id:N",
                sort=alt.EncodingSortField(
                    field="total_time_seconds", order="ascending"
                ),
                title=None,
                axis=alt.Axis(labelFontSize=8, labelLimit=300),
            ),
            color=alt.Color(
                "total_time_seconds:Q",
                scale=alt.Scale(
                    type="log",
                    scheme="tealblues",
                    reverse=True,
                    domainMin=total_min,
                    domainMax=total_max,
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("benchmark_id:N", title="System"),
                alt.Tooltip(
                    "total_time_seconds:Q", title="Total Time (s)", format=".3f"
                ),
            ],
        )
        .properties(
            width=500, height=max(num_systems * 16, 150), title="Total (All Queries)"
        )
        .add_params(total_zoom)
    )

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
                y=alt.Y(
                    "benchmark_id:N",
                    sort=alt.EncodingSortField(field="time_seconds", order="ascending"),
                    title=None,
                    axis=alt.Axis(labelFontSize=8, labelLimit=300),
                ),
                color=alt.Color(
                    "time_seconds:Q",
                    scale=alt.Scale(
                        type="log",
                        scheme="greens",
                        reverse=True,
                        domainMin=0.01,
                        domainMax=300,
                    ),
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

    return alt.vconcat(total_chart, *charts).resolve_scale(color="shared")


def speedup_chart(df: pd.DataFrame, bid1: str, bid2: str) -> alt.Chart:
    """
    Head-to-head diverging bar chart.

    X axis: signed linear speedup multiplier
      > 0  → bid1 is faster (bar goes right, green)
      < 0  → bid2 is faster (bar goes left, red)
      Tick labels float in the vertical center of the chart.

    Y axis: query number, sorted Q0–Q42, with a "Total" row at the bottom.

    Each bar has a speedup multiplier label (e.g. "4.2×") at its tip.
    """
    import numpy as np

    # Deduplicate: if the benchmark was run multiple times keep the best time per query
    def _best(subset, col):
        return (
            subset[["query_number", "time_seconds"]]
            .rename(columns={"time_seconds": col})
            .groupby("query_number", as_index=False)[col]
            .min()
        )

    d1 = _best(df[df["benchmark_id"] == bid1], "t1")
    d2 = _best(df[df["benchmark_id"] == bid2], "t2")
    merged = d1.merge(d2, on="query_number").dropna(subset=["t1", "t2"])
    merged = merged[merged["t1"] > 0].copy()

    merged["ratio"] = merged["t2"] / merged["t1"]
    merged["speedup_linear"] = merged["ratio"].apply(
        lambda r: (r if r > 1 else (-(1 / r) if r < 1 else 0.0))
    )
    merged["bar_label"] = merged["ratio"].apply(
        lambda r: f"{r:.1f}×" if r >= 1 else f"{1 / r:.1f}×"
    )
    merged["faster"] = merged["speedup_linear"].apply(
        lambda x: bid1 if x > 0 else (bid2 if x < 0 else "equal")
    )
    merged["bar_color"] = merged["speedup_linear"].apply(
        lambda x: "#22c55e" if x >= 0 else "#ef4444"
    )
    merged["q_num"] = merged["query_number"].str.replace("Q", "").astype(int)
    query_order = [f"Q{i}" for i in sorted(merged["q_num"].unique())]

    # Total row — sum of all matched query times
    total_t1 = merged["t1"].sum()
    total_t2 = merged["t2"].sum()
    total_ratio = total_t2 / total_t1
    total_speedup_linear = (
        total_ratio
        if total_ratio > 1
        else (-(1 / total_ratio) if total_ratio < 1 else 0.0)
    )
    total_row = pd.DataFrame(
        [
            {
                "query_number": "Total",
                "t1": total_t1,
                "t2": total_t2,
                "speedup_linear": total_speedup_linear,
                "ratio": total_ratio,
                "bar_label": f"{total_ratio:.1f}×"
                if total_ratio >= 1
                else f"{1 / total_ratio:.1f}×",
                "faster": bid1
                if total_speedup_linear > 0
                else (bid2 if total_speedup_linear < 0 else "equal"),
                "bar_color": "#1e40af",  # blue for total
                "q_num": 999,
            }
        ]
    )
    plot_df = pd.concat([merged, total_row], ignore_index=True)
    full_order = query_order + ["Total"]

    chart_height = max(len(full_order) * 14, 400)

    DISPLAY_CAP = 700
    TICK_STEP = 50
    max_abs = max(
        abs(plot_df["speedup_linear"].max()), abs(plot_df["speedup_linear"].min()), 1
    )
    display_max = min(max_abs, DISPLAY_CAP)
    x_domain = [-display_max, display_max]
    x_scale = alt.Scale(domain=x_domain)

    has_clipped = (plot_df["speedup_linear"].abs() > DISPLAY_CAP).any()

    # Tick marks — linear step size.
    tick_rows = [{"x": 0, "label": "1×"}]
    v = TICK_STEP
    while v <= display_max:
        label = f"{int(v)}×"
        tick_rows.append({"x": v, "label": label})
        tick_rows.append({"x": -v, "label": label})
        v += TICK_STEP
    tick_df = pd.DataFrame(tick_rows)

    # --- Layers ---

    # Faint vertical gridlines at tick positions
    grids = (
        alt.Chart(tick_df)
        .mark_rule(color="#e5e7eb", strokeWidth=1)
        .encode(x=alt.X("x:Q", scale=x_scale))
    )

    # Bars
    bars = (
        alt.Chart(plot_df)
        .mark_bar(clip=True)
        .encode(
            x=alt.X(
                "speedup_linear:Q",
                scale=x_scale,
                axis=alt.Axis(labels=False, title=None, grid=False, ticks=False),
            ),
            y=alt.Y(
                "query_number:N",
                sort=full_order,
                title="Query",
                axis=alt.Axis(labelFontSize=9),
            ),
            color=alt.Color(
                "bar_color:N",
                scale=alt.Scale(
                    domain=["#22c55e", "#ef4444", "#1e40af"],
                    range=["#22c55e", "#ef4444", "#1e40af"],
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("query_number:N", title="Query"),
                alt.Tooltip("t1:Q", title=f"{bid1} (s)", format=".3f"),
                alt.Tooltip("t2:Q", title=f"{bid2} (s)", format=".3f"),
                alt.Tooltip("bar_label:N", title="Speedup"),
                alt.Tooltip("faster:N", title="Faster"),
            ],
        )
        .properties(
            width=600,
            height=chart_height,
            title=alt.TitleParams(
                text=f"{bid2}  vs  {bid1}",
                subtitle=[
                    f"← {bid2} faster  |  {bid1} faster →",
                    *(
                        [f"bars clipped at {DISPLAY_CAP}× — hover for exact ratio"]
                        if has_clipped
                        else []
                    ),
                ],
                subtitleColor="#6b7280",
                subtitleFontSize=11,
            ),
        )
    )

    # Per-bar speedup labels — keep labels inside near chart edges.
    label_df = plot_df.copy()
    label_df["clipped"] = label_df["speedup_linear"].abs() > DISPLAY_CAP
    label_df["label_x"] = label_df["speedup_linear"].clip(-display_max, display_max)

    near_edge = label_df["speedup_linear"].abs() >= (display_max * 0.9)
    label_df["inside"] = label_df["clipped"] | near_edge

    pos_df = label_df[(label_df["speedup_linear"] >= 0) & ~label_df["inside"]]
    pos_inside_df = label_df[(label_df["speedup_linear"] >= 0) & label_df["inside"]]
    neg_df = label_df[(label_df["speedup_linear"] < 0) & ~label_df["inside"]]
    neg_inside_df = label_df[(label_df["speedup_linear"] < 0) & label_df["inside"]]

    def _lbl(data, align, dx):
        return (
            alt.Chart(data)
            .mark_text(
                fontSize=8, fontWeight="bold", color="#111827", align=align, dx=dx
            )
            .encode(
                x=alt.X("label_x:Q", scale=x_scale),
                y=alt.Y("query_number:N", sort=full_order),
                text=alt.Text("bar_label:N"),
            )
        )

    bar_labels_pos = _lbl(pos_df, "left", 3)
    bar_labels_pos_clip = _lbl(pos_inside_df, "right", -3)
    bar_labels_neg = _lbl(neg_df, "right", -3)
    bar_labels_neg_clip = _lbl(neg_inside_df, "left", 3)

    # Axis tick labels pinned to the bottom of the chart
    floating_axis = (
        alt.Chart(tick_df)
        .mark_text(fontSize=9, color="#6b7280", baseline="top", fontWeight="bold")
        .encode(
            x=alt.X("x:Q", scale=x_scale),
            text=alt.Text("label:N"),
            y=alt.value(chart_height + 2),
        )
    )

    # Zero / centre line
    zero_rule = (
        alt.Chart(pd.DataFrame({"x": [0]}))
        .mark_rule(color="#111827", strokeWidth=1.5)
        .encode(x=alt.X("x:Q", scale=x_scale))
    )

    return (
        alt.layer(
            grids,
            bars,
            bar_labels_pos,
            bar_labels_pos_clip,
            bar_labels_neg,
            bar_labels_neg_clip,
            floating_axis,
            zero_rule,
        )
        .configure_axis(labelFontSize=10)
        .configure_view(stroke=None)
    )


def main():
    parser = argparse.ArgumentParser(
        description="Generate ClickBench comparison charts"
    )
    parser.add_argument(
        "--csv", default=str(DEFAULT_CSV), help="Path to CSV results file"
    )
    parser.add_argument(
        "--run",
        default="hot",
        choices=["hot", "cold", "all"],
        help="Which run to plot (default: hot)",
    )
    parser.add_argument(
        "--top", type=int, default=None, help="Show only top N fastest systems"
    )
    parser.add_argument(
        "--provider",
        nargs="+",
        default=None,
        metavar="PROVIDER",
        help="Filter to one or more providers (e.g. --provider supabase_ducklake pg_duckdb)",
    )
    parser.add_argument(
        "--compare",
        nargs=2,
        metavar="BENCHMARK_ID",
        help="Head-to-head speedup chart for two benchmark IDs",
    )
    parser.add_argument(
        "--output", default=str(SCRIPT_DIR / "results"), help="Output directory"
    )
    args = parser.parse_args()

    df = load(Path(args.csv), args.run, args.provider)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"Loaded {len(df)} rows, {df['benchmark_id'].nunique()} systems, {df['query_number'].nunique()} queries"
    )
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
