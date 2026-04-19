from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Sequence

import pandas as pd


try:
    from src.metrics import (
        build_fact_user_daily,
        build_agg_daily_kpis,
        build_agg_retention_cohort,
        build_agg_funnel_daily,
        build_agg_lifecycle_daily,
    )
except ImportError:
    from metrics import (  # type: ignore
        build_fact_user_daily,
        build_agg_daily_kpis,
        build_agg_retention_cohort,
        build_agg_funnel_daily,
        build_agg_lifecycle_daily,
    )


DEFAULT_RETENTION_DAYS = (1, 7, 30)
DEFAULT_GROUP_COLS = ("variant",)


class Progress:
    def __init__(self, total_steps: int, label: str = "pipeline"):
        self.total_steps = total_steps
        self.label = label
        self.t0 = time.time()
        self.current_step = 0

    def start_step(self, step_name: str) -> float:
        self.current_step += 1
        elapsed = time.time() - self.t0
        print(
            f"\n[{self.label}] Step {self.current_step}/{self.total_steps} START "
            f"- {step_name} | elapsed {elapsed:7.1f}s",
            flush=True,
        )
        return time.time()

    def end_step(self, step_name: str, step_t0: float, extra: str = "") -> None:
        step_elapsed = time.time() - step_t0
        total_elapsed = time.time() - self.t0
        msg = (
            f"[{self.label}] Step {self.current_step}/{self.total_steps} DONE  "
            f"- {step_name} | step {step_elapsed:7.1f}s | total {total_elapsed:7.1f}s"
        )
        if extra:
            msg += f" | {extra}"
        print(msg, flush=True)


def ensure_dir(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def parse_group_cols(value: str | None) -> Sequence[str]:
    if value is None:
        return DEFAULT_GROUP_COLS
    value = value.strip()
    if value == "":
        return tuple()
    return tuple(v.strip() for v in value.split(",") if v.strip())


def parse_retention_days(value: str | None) -> Sequence[int]:
    if value is None:
        return DEFAULT_RETENTION_DAYS
    value = value.strip()
    if value == "":
        return DEFAULT_RETENTION_DAYS
    return tuple(int(v.strip()) for v in value.split(",") if v.strip())


def read_raw_tables(data_dir: str | Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data_dir = Path(data_dir)

    users_path = data_dir / "users.csv"
    sessions_path = data_dir / "sessions.csv"
    events_path = data_dir / "events.csv"
    lessons_path = data_dir / "lessons.csv"

    missing = [str(p) for p in [users_path, sessions_path, events_path, lessons_path] if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing required raw files:\n" + "\n".join(f" - {m}" for m in missing)
        )

    users = pd.read_csv(users_path)
    sessions = pd.read_csv(sessions_path)
    events = pd.read_csv(events_path)
    lessons = pd.read_csv(lessons_path)

    return users, sessions, events, lessons


def print_table_summary(name: str, df: pd.DataFrame) -> None:
    print(f"{name:>22}: {df.shape[0]:>12,} rows x {df.shape[1]:>4,} cols", flush=True)


def validate_minimum_columns(users: pd.DataFrame, sessions: pd.DataFrame, events: pd.DataFrame) -> None:
    required_users = {"user_id", "signup_date"}
    required_sessions = {"session_id", "user_id", "session_start", "session_end", "session_duration_sec"}
    required_events = {"event_id", "user_id", "event_time", "event_name"}

    miss_users = required_users - set(users.columns)
    miss_sessions = required_sessions - set(sessions.columns)
    miss_events = required_events - set(events.columns)

    errors = []
    if miss_users:
        errors.append(f"users missing columns: {sorted(miss_users)}")
    if miss_sessions:
        errors.append(f"sessions missing columns: {sorted(miss_sessions)}")
    if miss_events:
        errors.append(f"events missing columns: {sorted(miss_events)}")

    if errors:
        raise ValueError("Input raw tables failed minimum schema validation:\n" + "\n".join(errors))


def build_all_derived_tables(
    users: pd.DataFrame,
    sessions: pd.DataFrame,
    events: pd.DataFrame,
    *,
    retention_days: Sequence[int] = DEFAULT_RETENTION_DAYS,
    group_cols: Sequence[str] = DEFAULT_GROUP_COLS,
    date_min: str | None = None,
    date_max: str | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Build the five dashboard-serving derived datasets.

    This version prints progress at each table-building step.
    """
    prog = Progress(total_steps=5, label="build_derived_tables")

    t = prog.start_step("fact_user_daily")
    fact_user_daily = build_fact_user_daily(
        users=users,
        events=events,
        sessions=sessions,
        active_event_names=None,
        date_min=date_min,
        date_max=date_max,
    )
    prog.end_step(
        "fact_user_daily",
        t,
        extra=f"shape={fact_user_daily.shape[0]:,}x{fact_user_daily.shape[1]}",
    )

    t = prog.start_step("agg_daily_kpis")
    agg_daily_kpis = build_agg_daily_kpis(
        fact_user_daily=fact_user_daily,
        users=users,
        events=events,
        group_cols=tuple(group_cols),
        active_event_names=None,
    )
    prog.end_step(
        "agg_daily_kpis",
        t,
        extra=f"shape={agg_daily_kpis.shape[0]:,}x{agg_daily_kpis.shape[1]}",
    )

    t = prog.start_step("agg_retention_cohort")
    agg_retention_cohort = build_agg_retention_cohort(
        users=users,
        events=events,
        n_days=tuple(retention_days),
        active_event_names=None,
        by_variant=("variant" in group_cols),
    )
    prog.end_step(
        "agg_retention_cohort",
        t,
        extra=f"shape={agg_retention_cohort.shape[0]:,}x{agg_retention_cohort.shape[1]}",
    )

    t = prog.start_step("agg_funnel_daily")
    agg_funnel_daily = build_agg_funnel_daily(
        events=events,
        group_cols=tuple(group_cols),
    )
    prog.end_step(
        "agg_funnel_daily",
        t,
        extra=f"shape={agg_funnel_daily.shape[0]:,}x{agg_funnel_daily.shape[1]}",
    )

    t = prog.start_step("agg_lifecycle_daily")
    agg_lifecycle_daily = build_agg_lifecycle_daily(
        fact_user_daily=fact_user_daily,
        group_cols=tuple(group_cols),
    )
    prog.end_step(
        "agg_lifecycle_daily",
        t,
        extra=f"shape={agg_lifecycle_daily.shape[0]:,}x{agg_lifecycle_daily.shape[1]}",
    )

    return {
        "fact_user_daily": fact_user_daily,
        "agg_daily_kpis": agg_daily_kpis,
        "agg_retention_cohort": agg_retention_cohort,
        "agg_funnel_daily": agg_funnel_daily,
        "agg_lifecycle_daily": agg_lifecycle_daily,
    }


def write_tables(
    tables: dict[str, pd.DataFrame],
    out_dir: str | Path,
    *,
    file_format: str = "csv",
) -> None:
    out_dir = Path(out_dir)
    ensure_dir(out_dir)

    file_format = file_format.lower().strip()
    if file_format not in {"csv", "parquet"}:
        raise ValueError("file_format must be 'csv' or 'parquet'")

    total = len(tables)
    t0 = time.time()

    for i, (name, df) in enumerate(tables.items(), start=1):
        step_t0 = time.time()

        if file_format == "csv":
            path = out_dir / f"{name}.csv"
            df.to_csv(path, index=False)
        else:
            path = out_dir / f"{name}.parquet"
            df.to_parquet(path, index=False)

        elapsed = time.time() - step_t0
        total_elapsed = time.time() - t0
        print(
            f"[write_tables] {i}/{total} wrote {name:<22} -> {path} "
            f"| shape={df.shape[0]:,}x{df.shape[1]} "
            f"| step {elapsed:6.1f}s | total {total_elapsed:6.1f}s",
            flush=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build dashboard-serving derived tables from raw growth-model CSVs."
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        default="data",
        help="Directory containing raw users.csv / sessions.csv / events.csv / lessons.csv",
    )
    parser.add_argument(
        "--out_dir",
        type=str,
        default="data/derived",
        help="Directory to write derived tables",
    )
    parser.add_argument(
        "--retention_days",
        type=str,
        default="1,7,30",
        help="Comma-separated retention horizons, e.g. '1,7,30'",
    )
    parser.add_argument(
        "--group_cols",
        type=str,
        default="variant",
        help="Comma-separated grouping columns for aggregate tables, e.g. 'variant' or 'variant,country'",
    )
    parser.add_argument(
        "--date_min",
        type=str,
        default=None,
        help="Optional lower bound date (YYYY-MM-DD) for fact_user_daily",
    )
    parser.add_argument(
        "--date_max",
        type=str,
        default=None,
        help="Optional upper bound date (YYYY-MM-DD) for fact_user_daily",
    )
    parser.add_argument(
        "--format",
        type=str,
        default="csv",
        choices=["csv", "parquet"],
        help="Output format",
    )

    args = parser.parse_args()

    overall = Progress(total_steps=4, label="main")

    t = overall.start_step("parse arguments")
    retention_days = parse_retention_days(args.retention_days)
    group_cols = parse_group_cols(args.group_cols)
    overall.end_step(
        "parse arguments",
        t,
        extra=f"retention_days={tuple(retention_days)}, group_cols={tuple(group_cols)}",
    )

    t = overall.start_step("read raw tables")
    users, sessions, events, lessons = read_raw_tables(args.data_dir)
    validate_minimum_columns(users, sessions, events)
    overall.end_step(
        "read raw tables",
        t,
        extra=(
            f"users={users.shape[0]:,}, sessions={sessions.shape[0]:,}, "
            f"events={events.shape[0]:,}, lessons={lessons.shape[0]:,}"
        ),
    )

    print("\nRaw table summary", flush=True)
    print_table_summary("users", users)
    print_table_summary("sessions", sessions)
    print_table_summary("events", events)
    print_table_summary("lessons", lessons)

    t = overall.start_step("build derived tables")
    tables = build_all_derived_tables(
        users=users,
        sessions=sessions,
        events=events,
        retention_days=retention_days,
        group_cols=group_cols,
        date_min=args.date_min,
        date_max=args.date_max,
    )
    overall.end_step(
        "build derived tables",
        t,
        extra=f"created={len(tables)} tables",
    )

    print("\nDerived table summary", flush=True)
    for name, df in tables.items():
        print_table_summary(name, df)

    t = overall.start_step("write outputs")
    write_tables(
        tables=tables,
        out_dir=args.out_dir,
        file_format=args.format,
    )
    overall.end_step(
        "write outputs",
        t,
        extra=f"out_dir={args.out_dir}, format={args.format}",
    )

    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()