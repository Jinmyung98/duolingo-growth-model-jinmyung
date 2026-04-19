from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text


RAW_TABLES = {
    "users": "users.csv",
    "sessions": "sessions.csv",
    "events": "events.csv",
    "lessons": "lessons.csv",
}

DERIVED_TABLES = {
    "fact_user_daily": "fact_user_daily.csv",
    "agg_daily_kpis": "agg_daily_kpis.csv",
    "agg_retention_cohort": "agg_retention_cohort.csv",
    "agg_funnel_daily": "agg_funnel_daily.csv",
    "agg_lifecycle_daily": "agg_lifecycle_daily.csv",
}


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


def print_table_summary(name: str, df: pd.DataFrame) -> None:
    print(f"{name:>22}: {df.shape[0]:>12,} rows x {df.shape[1]:>4,} cols", flush=True)


def make_engine(
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
):
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url, future=True)


def resolve_input_path(base_dir: Path, filename: str, file_format: str) -> Path:
    suffix = ".csv" if file_format == "csv" else ".parquet"
    return base_dir / filename.replace(".csv", suffix)


def read_table(path: Path, file_format: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    if file_format == "csv":
        return pd.read_csv(path)
    if file_format == "parquet":
        return pd.read_parquet(path)

    raise ValueError("file_format must be 'csv' or 'parquet'")


def maybe_parse_datetimes(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse common date/datetime columns so they land in Postgres with better types.
    """
    out = df.copy()

    datetime_cols = [
        "signup_time",
        "session_start",
        "session_end",
        "event_time",
    ]
    date_cols = [
        "signup_date",
        "event_date",
        "date",
        "cohort_date",
    ]

    for col in datetime_cols:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")

    for col in date_cols:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.date

    return out


def validate_files_exist(
    raw_dir: Path,
    derived_dir: Path,
    *,
    include_raw: bool,
    include_derived: bool,
    file_format: str,
) -> None:
    missing: List[str] = []

    if include_raw:
        for name, filename in RAW_TABLES.items():
            path = resolve_input_path(raw_dir, filename, file_format)
            if not path.exists():
                missing.append(str(path))

    if include_derived:
        for name, filename in DERIVED_TABLES.items():
            path = resolve_input_path(derived_dir, filename, file_format)
            if not path.exists():
                missing.append(str(path))

    if missing:
        raise FileNotFoundError(
            "Missing input files:\n" + "\n".join(f" - {m}" for m in missing)
        )


def load_one_table(
    *,
    engine,
    table_name: str,
    file_path: Path,
    file_format: str,
    schema: Optional[str],
    if_exists: str,
    chunksize: int,
    method: str,
) -> None:
    step_t0 = time.time()

    print(f"[load] reading {table_name} from {file_path}", flush=True)
    df = read_table(file_path, file_format)
    df = maybe_parse_datetimes(table_name, df)
    print_table_summary(table_name, df)

    print(
        f"[load] writing {table_name} -> postgres "
        f"(if_exists={if_exists}, chunksize={chunksize}, method={method})",
        flush=True,
    )

    to_sql_method = "multi" if method == "multi" else None

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        chunksize=chunksize,
        method=to_sql_method,
    )

    elapsed = time.time() - step_t0
    print(
        f"[load] DONE {table_name:<22} | rows={len(df):,} cols={df.shape[1]} | {elapsed:7.1f}s",
        flush=True,
    )


def analyze_table(engine, table_name: str, schema: Optional[str]) -> None:
    fq = f'"{table_name}"' if schema is None else f'"{schema}"."{table_name}"'
    with engine.begin() as conn:
        conn.execute(text(f"ANALYZE {fq};"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load raw and derived growth-model tables into PostgreSQL."
    )

    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", type=str, default="growth_model")
    parser.add_argument("--user", type=str, default="superset_user")
    parser.add_argument("--password", type=str, required=True)

    parser.add_argument("--raw_dir", type=str, default="data")
    parser.add_argument("--derived_dir", type=str, default="data/derived")

    parser.add_argument("--include_raw", action="store_true")
    parser.add_argument("--include_derived", action="store_true")
    parser.add_argument(
        "--file_format",
        type=str,
        default="csv",
        choices=["csv", "parquet"],
    )
    parser.add_argument(
        "--if_exists",
        type=str,
        default="replace",
        choices=["fail", "replace", "append"],
        help="Table write mode. Use replace during development.",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=None,
        help="Optional target schema in PostgreSQL.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=50000,
        help="Rows per batch for pandas to_sql.",
    )
    parser.add_argument(
        "--method",
        type=str,
        default="multi",
        choices=["multi", "single"],
        help="'multi' is usually faster.",
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Run ANALYZE on loaded tables after import.",
    )

    args = parser.parse_args()

    # If neither flag is provided, load both.
    include_raw = args.include_raw or (not args.include_raw and not args.include_derived)
    include_derived = args.include_derived or (not args.include_raw and not args.include_derived)

    raw_dir = Path(args.raw_dir)
    derived_dir = Path(args.derived_dir)

    prog = Progress(total_steps=4, label="load_to_postgres")

    t = prog.start_step("validate input files")
    validate_files_exist(
        raw_dir,
        derived_dir,
        include_raw=include_raw,
        include_derived=include_derived,
        file_format=args.file_format,
    )
    prog.end_step(
        "validate input files",
        t,
        extra=f"include_raw={include_raw}, include_derived={include_derived}, format={args.file_format}",
    )

    t = prog.start_step("create postgres engine")
    engine = make_engine(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))
    prog.end_step(
        "create postgres engine",
        t,
        extra=f"{args.user}@{args.host}:{args.port}/{args.dbname}",
    )

    tables_to_load: Dict[str, Path] = {}

    if include_raw:
        for table_name, filename in RAW_TABLES.items():
            tables_to_load[table_name] = resolve_input_path(raw_dir, filename, args.file_format)

    if include_derived:
        for table_name, filename in DERIVED_TABLES.items():
            tables_to_load[table_name] = resolve_input_path(derived_dir, filename, args.file_format)

    t = prog.start_step("load tables")
    total_tables = len(tables_to_load)

    for i, (table_name, file_path) in enumerate(tables_to_load.items(), start=1):
        print(f"\n[load] {i}/{total_tables} table={table_name}", flush=True)
        load_one_table(
            engine=engine,
            table_name=table_name,
            file_path=file_path,
            file_format=args.file_format,
            schema=args.schema,
            if_exists=args.if_exists,
            chunksize=args.chunksize,
            method=args.method,
        )
        if args.analyze:
            print(f"[load] analyze {table_name}", flush=True)
            analyze_table(engine, table_name, args.schema)

    prog.end_step(
        "load tables",
        t,
        extra=f"loaded_tables={total_tables}",
    )

    t = prog.start_step("final verification")
    with engine.begin() as conn:
        for table_name in tables_to_load:
            fq = f'"{table_name}"' if args.schema is None else f'"{args.schema}"."{table_name}"'
            count = conn.execute(text(f"SELECT COUNT(*) FROM {fq}")).scalar_one()
            print(f"[verify] {table_name:<22} rows={count:,}", flush=True)
    prog.end_step("final verification", t)

    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()