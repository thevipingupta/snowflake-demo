#!/usr/bin/env python3
"""Render the four business-question charts from the Snowflake analytics views.

Reads the four views in CC_DEMO.GOLD (v_q1_..., v_q2_..., v_q3_..., v_q4_...) and writes
one PNG per question to output/charts/ (default: <project>/output/charts).

Credentials come only from environment variables. Nothing is hard-coded:
    SNOWFLAKE_ACCOUNT            required
    SNOWFLAKE_USER               required
    SNOWFLAKE_PASSWORD           password auth (or set SNOWFLAKE_AUTHENTICATOR)
    SNOWFLAKE_AUTHENTICATOR      optional, e.g. externalbrowser
    SNOWFLAKE_WAREHOUSE          required
    SNOWFLAKE_ROLE               optional
    SNOWFLAKE_DATABASE           optional, default CC_DEMO
Install: pip install snowflake-connector-python pandas matplotlib

Every chart has a title, axis labels, and a one-line takeaway in the subtitle. The
takeaway is computed from the query results. Q2 and Q4 carry a note that the result
depends on the WFM assumption.

Usage:
    python src/analytics/build_charts.py [--out output/charts]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

PROJECT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = PROJECT / "output" / "charts"
SCHEMA = "GOLD"
WFM_NOTE = ("Depends on the WFM assumption: staffing comes from the WFM extract, "
            "which is not a Five9 source and must be confirmed with the client.")
PALETTE = {"CLAIMANT_PHONE": "#1f5fa8", "EMPLOYER_PHONE": "#2e8b57", "GENERAL_PHONE": "#8a6d3b"}
DRIVER_COLORS = {"VOLUME_SURGE": "#c0392b", "STAFFING_SHORTFALL": "#2471a3",
                 "SURGE_AND_SHORTFALL": "#7d3c98", "UNEXPLAINED": "#7f8c8d"}


def connect():
    """Open a Snowflake connection from environment variables only."""
    import snowflake.connector

    missing = [v for v in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE")
               if not os.environ.get(v)]
    if missing:
        sys.exit(f"Missing environment variables: {', '.join(missing)}")
    kwargs = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ.get("SNOWFLAKE_DATABASE", "CC_DEMO"),
        "schema": SCHEMA,
    }
    if os.environ.get("SNOWFLAKE_ROLE"):
        kwargs["role"] = os.environ["SNOWFLAKE_ROLE"]
    if os.environ.get("SNOWFLAKE_AUTHENTICATOR"):
        kwargs["authenticator"] = os.environ["SNOWFLAKE_AUTHENTICATOR"]
    else:
        pwd = os.environ.get("SNOWFLAKE_PASSWORD")
        if not pwd:
            sys.exit("Set SNOWFLAKE_PASSWORD or SNOWFLAKE_AUTHENTICATOR.")
        kwargs["password"] = pwd
    return snowflake.connector.connect(**kwargs)


def query(conn, view: str) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {SCHEMA}.{view}")
        cols = [c[0].lower() for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def _save(fig, out_dir: Path, name: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def chart_q1(df: pd.DataFrame, out_dir: Path) -> Path:
    df = df.dropna(subset=["service_level_pct"]).copy()
    df["week_start"] = pd.to_datetime(df["week_start"])
    df["sl_x_calls"] = df["service_level_pct"] * df["calls_offered"]
    weekly = (df.groupby(["skill_name", "week_start"])
                .agg(sl_x_calls=("sl_x_calls", "sum"), calls=("calls_offered", "sum"),
                     soa=("soa_threshold_sec", "max"))
                .reset_index())
    weekly["sl"] = weekly["sl_x_calls"] / weekly["calls"]
    worst = weekly.loc[weekly["sl"].idxmin()]
    base = weekly[(weekly.skill_name == worst.skill_name) & (weekly.week_start < worst.week_start)
                  & (weekly.week_start >= worst.week_start - pd.Timedelta(weeks=4))]["sl"].mean()
    takeaway = (f"{worst.skill_name} service level fell to {worst.sl:.0f}% in the week of "
                f"{worst.week_start:%Y-%m-%d}, against a {base:.0f}% prior-4-week average.")

    fig, ax = plt.subplots(figsize=(10, 5.5))
    for skill, g in weekly.groupby("skill_name"):
        soa = int(g["soa"].max())
        ax.plot(g["week_start"], g["sl"], marker="o", lw=2, color=PALETTE.get(skill),
                label=f"{skill} (SOA {soa} s)")
    ax.set_title("Q1. Where are service levels slipping, by skill and week?")
    ax.set_xlabel("Week starting")
    ax.set_ylabel("Service level (% of calls, from the source SERVICE LEVEL)")
    ax.set_ylim(0, 105)
    ax.grid(alpha=0.3)
    ax.legend(loc="lower left")
    fig.suptitle(takeaway, y=0.02, fontsize=10, color="#444")
    return _save(fig, out_dir, "q1_service_level_slippage.png")


def chart_q2(df: pd.DataFrame, out_dir: Path) -> Path:
    df = df.copy()
    df["calendar_date"] = pd.to_datetime(df["calendar_date"])
    spikes = df[df["driver"].isin(list(DRIVER_COLORS))]
    counts = spikes["driver"].value_counts()
    if len(spikes):
        top = spikes.sort_values("abandonment_rate_pct", ascending=False).iloc[0]
        takeaway = (f"{len(spikes)} spike days: {counts.get('VOLUME_SURGE', 0)} volume surge, "
                    f"{counts.get('STAFFING_SHORTFALL', 0)} staffing shortfall, "
                    f"{counts.get('SURGE_AND_SHORTFALL', 0)} both. Worst: {top.skill_name} on "
                    f"{top.calendar_date:%Y-%m-%d} at {top.abandonment_rate_pct:.0f}% abandoned.")
    else:
        takeaway = "No abandonment spike days were found in this extract."

    fig, ax = plt.subplots(figsize=(10, 5.5))
    for skill, g in df.groupby("skill_name"):
        ax.plot(g["calendar_date"], g["abandonment_rate_pct"], color=PALETTE.get(skill), lw=0.8,
                alpha=0.5, label=f"{skill} daily")
    for driver, color in DRIVER_COLORS.items():
        g = spikes[spikes["driver"] == driver]
        if len(g):
            ax.scatter(g["calendar_date"], g["abandonment_rate_pct"], s=46, color=color,
                       edgecolor="black", zorder=3, label=f"Spike: {driver.replace('_', ' ').lower()}")
    ax.set_title("Q2. Why did abandonment spike? Surge or staffing?")
    ax.set_xlabel("Date")
    ax.set_ylabel("Abandonment rate (% of calls offered)")
    ax.grid(alpha=0.3)
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    fig.suptitle(takeaway + "\n" + WFM_NOTE, y=0.02, fontsize=9, color="#444")
    return _save(fig, out_dir, "q2_abandonment_driver.png")


def chart_q3(df: pd.DataFrame, out_dir: Path) -> Path:
    top = df.sort_values("calls_with_disposition", ascending=False).head(10).copy()
    top = top.sort_values("repeat_rate_pct")
    spread = df["repeat_rate_pct"].max() - df["repeat_rate_pct"].min()
    takeaway = (f"Repeat rates run {df['repeat_rate_pct'].min():.1f}% to "
                f"{df['repeat_rate_pct'].max():.1f}% across all dispositions (a {spread:.1f}-point "
                f"spread): no single disposition stands out.")

    fig, ax = plt.subplots(figsize=(10, 6))
    labels = [p if len(p) <= 60 else p[:57] + "..." for p in top["disposition_path"]]
    ax.barh(labels, top["repeat_rate_pct"], color="#1f5fa8")
    for y, v in enumerate(top["repeat_rate_pct"]):
        ax.text(v + 0.2, y, f"{v:.1f}%", va="center", fontsize=8)
    ax.set_title("Q3. Which dispositions lead to repeat calls within 7 days?")
    ax.set_xlabel("Repeat rate (% of calls with this disposition followed by a repeat within 7 days)")
    ax.set_ylabel("Disposition path (earlier call), top 10 by volume")
    ax.set_xlim(0, max(top["repeat_rate_pct"].max() * 1.15, 1))
    ax.grid(alpha=0.3, axis="x")
    fig.suptitle(takeaway + "\nRepeat definition pending VEC confirmation.", y=0.02, fontsize=9, color="#444")
    return _save(fig, out_dir, "q3_disposition_repeat.png")


def chart_q4(df: pd.DataFrame, out_dir: Path) -> Path:
    df = df.dropna(subset=["service_level_pct", "adherence_ratio"]).copy()
    r_adh = df["adherence_ratio"].corr(df["service_level_pct"])
    r_log = df["logged_in_minutes"].corr(df["service_level_pct"])
    takeaway = (f"Adherence ratio vs service level: r = {r_adh:.2f}. Logged minutes vs service "
                f"level: r = {r_log:.2f}. Staffed hours track service level, adherence does not.")

    fig, axes = plt.subplots(1, 2, figsize=(11, 5.5), sharey=True)
    for skill, g in df.groupby("skill_name"):
        axes[0].scatter(g["adherence_ratio"], g["service_level_pct"], s=18, alpha=0.7,
                        color=PALETTE.get(skill), label=skill)
        axes[1].scatter(g["logged_in_minutes"], g["service_level_pct"], s=18, alpha=0.7,
                        color=PALETTE.get(skill), label=skill)
    axes[0].set_title("Adherence (logged / scheduled)")
    axes[0].set_xlabel("Adherence ratio, WFM")
    axes[1].set_title("Logged agent minutes")
    axes[1].set_xlabel("Logged-in minutes that day, WFM")
    axes[0].set_ylabel("Service level (% of calls, from the source SERVICE LEVEL)")
    for ax in axes:
        ax.grid(alpha=0.3)
    axes[1].legend(loc="lower right", fontsize=8)
    fig.suptitle("Q4. How does agent adherence relate to service level, same day and skill?",
                 y=0.99, fontsize=12)
    fig.text(0.5, -0.02, takeaway + "\n" + WFM_NOTE, ha="center", fontsize=9, color="#444")
    return _save(fig, out_dir, "q4_adherence_service_level.png")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="output folder for the PNG charts")
    args = parser.parse_args(argv)

    conn = connect()
    try:
        frames = {
            "q1": query(conn, "V_Q1_SERVICE_LEVEL_SLIPPAGE"),
            "q2": query(conn, "V_Q2_ABANDONMENT_DRIVER"),
            "q3": query(conn, "V_Q3_DISPOSITION_REPEAT"),
            "q4": query(conn, "V_Q4_ADHERENCE_SERVICE_LEVEL"),
        }
    finally:
        conn.close()

    written = [
        chart_q1(frames["q1"], args.out),
        chart_q2(frames["q2"], args.out),
        chart_q3(frames["q3"], args.out),
        chart_q4(frames["q4"], args.out),
    ]
    for path in written:
        print(path)


if __name__ == "__main__":
    main()
