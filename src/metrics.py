# src/metrics.py
"""
Metrics layer for the Growth Model project.

Design goals:
- Single source of truth for KPI definitions (DAU/WAU/MAU/Retention/Funnel/etc.)
- Pure functions: no file I/O, deterministic outputs
- Tidy outputs (DataFrames) that are easy to plot / join / compare by variant

Tables (Phase 1 schema):
- events: event_id, user_id, event_time, event_date, event_name, variant, ...
- sessions: session_id, user_id, session_start, session_end, session_duration_sec, ...
- users: user_id, signup_date, variant, ...
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Union, List, Dict

import pandas as pd

import time


# -----------------------
# Defaults / Configuration
# -----------------------

DEFAULT_ACTIVE_EVENT_NAMES: List[str] = [
    "app_open",
    "lesson_started",
    "lesson_completed",
]

DEFAULT_FUNNEL_STEPS: List[str] = [
    "signup",
    "app_open",
    "lesson_started",
    "lesson_completed",
]


# -----------------------
# Helpers
# -----------------------

def _require_cols(df: pd.DataFrame, cols: Sequence[str], df_name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {missing}. Present: {list(df.columns)}")


def _to_date_series(s: pd.Series) -> pd.Series:
    """
    Convert a series to pandas datetime.date (normalized to date).
    Accepts: already date-like strings, datetime64, python date, etc.
    """
    # If already datetime64, keep; else parse
    dt = pd.to_datetime(s, errors="coerce")
    if dt.isna().any():
        # allow strings already like "2025-01-01" - parse should work; if not, error
        bad = s[dt.isna()].head(5).tolist()
        raise ValueError(f"Could not parse some dates. Examples: {bad}")
    return dt.dt.normalize().dt.date


def _prep_events(
    events: pd.DataFrame,
    *,
    active_event_names: Optional[Sequence[str]] = None,
    ensure_event_date: bool = True,
) -> pd.DataFrame:
    """
    Ensure events has standard columns and correct dtypes for metric computation.
    Returns a shallow copy with cleaned date.
    """
    _require_cols(events, ["user_id", "event_name"], "events")
    ev = events.copy()

    if ensure_event_date:
        if "event_date" in ev.columns:
            ev["event_date"] = _to_date_series(ev["event_date"])
        elif "event_time" in ev.columns:
            ev["event_date"] = _to_date_series(ev["event_time"])
        else:
            raise ValueError("events must have either 'event_date' or 'event_time'")

    if active_event_names is None:
        active_event_names = DEFAULT_ACTIVE_EVENT_NAMES

    ev["_is_active_event"] = ev["event_name"].isin(list(active_event_names))
    return ev


def _prep_sessions(sessions: pd.DataFrame) -> pd.DataFrame:
    _require_cols(sessions, ["user_id"], "sessions")
    se = sessions.copy()

    if "session_date" in se.columns:
        se["session_date"] = _to_date_series(se["session_date"])
    elif "session_start" in se.columns:
        se["session_date"] = _to_date_series(se["session_start"])
    else:
        raise ValueError("sessions must have either 'session_date' or 'session_start'")

    return se


def _group_keys(group_cols: Optional[Sequence[str]]) -> List[str]:
    return list(group_cols) if group_cols else []


def _unique_users_by_date(
    ev: pd.DataFrame,
    date_col: str,
    *,
    group_cols: Optional[Sequence[str]] = None,
    user_col: str = "user_id",
    value_name: str = "users",
) -> pd.DataFrame:
    keys_in = [date_col] + _group_keys(group_cols)

    out = (
        ev.groupby(keys_in, as_index=False)[user_col]
          .nunique()
          .rename(columns={user_col: value_name, date_col: "date"})
    )

    # Keep date as python date (normalized)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize().dt.date
    if out["date"].isna().any():
        bad = out.loc[out["date"].isna(), "date"].head(5).tolist()
        raise ValueError(f"Could not parse some dates in '{date_col}'. Examples: {bad}")

    keys_out = ["date"] + _group_keys(group_cols)
    return out.sort_values(keys_out).reset_index(drop=True)


def _trailing_window_active_users(
    ev_active: pd.DataFrame,
    *,
    window_days: int,
    group_cols: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """
    Compute trailing-window active users (WAU/MAU) using exact definition:
    user counts if they had >=1 active event in [t-(window_days-1), t].

    Implementation approach:
    - Work on unique (user, date, group) active days
    - For each date t, include users active in the trailing range
    - Efficient enough for moderate synthetic datasets
    """
    if window_days <= 0:
        raise ValueError("window_days must be positive")

    keys = _group_keys(group_cols)

    # Unique active user-days
    ud = ev_active.loc[ev_active["_is_active_event"], ["user_id", "event_date"] + keys].drop_duplicates()
    ud = ud.rename(columns={"event_date": "date"})
    ud["date"] = pd.to_datetime(ud["date"]).dt.date

    if ud.empty:
        # Return empty with expected columns
        cols = ["date"] + keys + [f"au_{window_days}d"]
        return pd.DataFrame(columns=cols)

    # Build full date index per group (so WAU exists even on days with zero active events)
    # Range is global per group for simplicity
    result_rows = []
    for gvals, gdf in ud.groupby(keys, dropna=False) if keys else [(None, ud)]:
        gdf = gdf.sort_values("date")
        all_dates = pd.date_range(gdf["date"].min(), gdf["date"].max(), freq="D").date

        # Map date -> set of users active on that date (small sets)
        by_date = gdf.groupby("date")["user_id"].apply(set).to_dict()

        # Sliding window of sets
        window_sets: List[set] = []
        union_set: set = set()

        # We'll maintain a queue of sets corresponding to last `window_days` dates.
        for t in all_dates:
            todays = by_date.get(t, set())
            window_sets.append(todays)
            union_set |= todays

            if len(window_sets) > window_days:
                dropped = window_sets.pop(0)
                # If dropped users might still exist in other sets, we must recompute union safely.
                # Recompute when needed (still ok for moderate data).
                if dropped:
                    union_set = set().union(*window_sets) if window_sets else set()

            row = {"date": t, f"au_{window_days}d": len(union_set)}
            if keys:
                if not isinstance(gvals, tuple):
                    gvals = (gvals,)
                row.update({k: v for k, v in zip(keys, gvals)})
            result_rows.append(row)

    out = pd.DataFrame(result_rows)
    return out.sort_values(["date"] + keys).reset_index(drop=True)

def _enrich_with_user_groups(
    df: pd.DataFrame,
    users: Optional[pd.DataFrame],
    *,
    required_group_cols: Sequence[str],
    df_name: str,
) -> pd.DataFrame:
    """
    Ensure group columns exist on df. If missing, attach from users via user_id.
    """
    out = df.copy()
    missing = [c for c in required_group_cols if c not in out.columns]

    if not missing:
        return out

    if users is None:
        raise ValueError(
            f"{df_name} is missing grouping columns {missing}. "
            f"Provide users to enrich {df_name} by user_id."
        )

    _require_cols(users, ["user_id"] + list(missing), "users")
    u = users[["user_id"] + list(missing)].drop_duplicates(subset=["user_id"])

    out = out.merge(u, on="user_id", how="left")

    still_missing = [c for c in required_group_cols if c not in out.columns]
    if still_missing:
        raise ValueError(f"Could not enrich {df_name} with grouping columns: {still_missing}")

    return out

def _assign_lifecycle_state_from_fact_with_progress(
    fact_user_daily: pd.DataFrame,
    *,
    verbose: bool = True,
    every: int = 500,
) -> pd.DataFrame:
    """
    Assign lifecycle_state directly from fact_user_daily, with progress logging.
    """

    def log(msg: str) -> None:
        if verbose:
            print(f"[lifecycle] {msg}", flush=True)

    _require_cols(
        fact_user_daily,
        ["date", "user_id", "signup_date", "is_active"],
        "fact_user_daily",
    )

    f = fact_user_daily.copy()
    f["date"] = _to_date_series(f["date"])
    f["signup_date"] = _to_date_series(f["signup_date"])
    f = f.sort_values(["user_id", "date"]).reset_index(drop=True)

    user_ids = f["user_id"].drop_duplicates().tolist()
    total_users = len(user_ids)
    out_parts = []
    t0 = time.time()

    for i, (uid, g) in enumerate(f.groupby("user_id", sort=False), start=1):
        g = g.sort_values("date").copy()
        active_dates = set(g.loc[g["is_active"].astype(bool), "date"])

        states = []
        for _, r in g.iterrows():
            t = r["date"]
            sdate = r["signup_date"]

            if t == sdate:
                states.append("New")
                continue

            t7_prev = set(
                pd.date_range(
                    pd.to_datetime(t) - pd.Timedelta(days=7),
                    pd.to_datetime(t) - pd.Timedelta(days=1),
                    freq="D",
                ).date
            )
            t30_prev = set(
                pd.date_range(
                    pd.to_datetime(t) - pd.Timedelta(days=30),
                    pd.to_datetime(t) - pd.Timedelta(days=1),
                    freq="D",
                ).date
            )
            t7_incl = set(
                pd.date_range(
                    pd.to_datetime(t) - pd.Timedelta(days=6),
                    pd.to_datetime(t),
                    freq="D",
                ).date
            )
            t30_incl = set(
                pd.date_range(
                    pd.to_datetime(t) - pd.Timedelta(days=29),
                    pd.to_datetime(t),
                    freq="D",
                ).date
            )

            A_today = t in active_dates
            w7_prev = len(active_dates & t7_prev)
            w30_prev = len(active_dates & t30_prev)
            w7_incl = len(active_dates & t7_incl)
            w30_incl = len(active_dates & t30_incl)

            if A_today:
                if w7_prev >= 1:
                    states.append("Current")
                elif w30_prev >= 1:
                    states.append("Reactivated")
                else:
                    states.append("Resurrected")
            else:
                if w7_incl >= 1:
                    states.append("AtRiskWAU")
                elif w30_incl >= 1:
                    states.append("AtRiskMAU")
                else:
                    states.append("Dormant")

        g["lifecycle_state"] = states
        out_parts.append(g)

        if i % every == 0 or i == total_users:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else float("inf")
            remaining = total_users - i
            eta = remaining / rate if rate > 0 else float("inf")
            log(f"{i:,}/{total_users:,} users processed | elapsed {elapsed:,.1f}s | ETA {eta:,.1f}s")

    return pd.concat(out_parts, ignore_index=True)

# -----------------------
# Core Metrics
# -----------------------

def compute_dau(
    events: pd.DataFrame,
    *,
    active_event_names: Optional[Sequence[str]] = None,
    group_cols: Optional[Sequence[str]] = ("variant",),
) -> pd.DataFrame:
    """
    DAU(t) = number of unique users with >=1 active event on date t.
    Returns: columns = [date] + group_cols + [dau]
    """
    ev = _prep_events(events, active_event_names=active_event_names)
    ev = ev.loc[ev["_is_active_event"]]
    out = _unique_users_by_date(ev, "event_date", group_cols=group_cols, value_name="dau")
    return out


def compute_wau(
    events: pd.DataFrame,
    *,
    active_event_names: Optional[Sequence[str]] = None,
    group_cols: Optional[Sequence[str]] = ("variant",),
) -> pd.DataFrame:
    """
    WAU(t) = unique users active at least once in [t-6, t].
    Returns: columns = [date] + group_cols + [wau]
    """
    ev = _prep_events(events, active_event_names=active_event_names)
    au = _trailing_window_active_users(ev, window_days=7, group_cols=group_cols)
    au = au.rename(columns={"au_7d": "wau"})
    return au


def compute_mau(
    events: pd.DataFrame,
    *,
    active_event_names: Optional[Sequence[str]] = None,
    group_cols: Optional[Sequence[str]] = ("variant",),
) -> pd.DataFrame:
    """
    MAU(t) = unique users active at least once in [t-29, t].
    Returns: columns = [date] + group_cols + [mau]
    """
    ev = _prep_events(events, active_event_names=active_event_names)
    au = _trailing_window_active_users(ev, window_days=30, group_cols=group_cols)
    au = au.rename(columns={"au_30d": "mau"})
    return au


def compute_retention(
    users: pd.DataFrame,
    events: pd.DataFrame,
    n_days: Union[int, Sequence[int]] = (1, 7, 30),
    *,
    active_event_names: Optional[Sequence[str]] = None,
    by_variant: bool = True,
    drop_incomplete: bool = True,
) -> pd.DataFrame:
    """
    Cohort retention:
    For cohort c (signup_date = c), user is retained on day n if active on (c + n).

    Output (long-form):
    cohort_date, day_n, cohort_size, retained_users, retention_rate, [variant]

    Parameters
    ----------
    drop_incomplete:
        If True, exclude cohort-day pairs whose target_date is beyond the observed
        event horizon. This avoids showing recent cohorts as artificial zero retention.
    """
    _require_cols(users, ["user_id", "signup_date"], "users")
    us = users.copy()
    us["signup_date"] = _to_date_series(us["signup_date"])

    ev = _prep_events(events, active_event_names=active_event_names)
    ev = ev.loc[
        ev["_is_active_event"],
        ["user_id", "event_date"] + (["variant"] if "variant" in ev.columns else [])
    ].drop_duplicates()

    ev = ev.rename(columns={"event_date": "active_date"})
    ev["active_date"] = pd.to_datetime(ev["active_date"]).dt.date

    if ev.empty:
        cols = ["cohort_date", "day_n", "cohort_size", "retained_users", "retention_rate"]
        if by_variant and "variant" in us.columns:
            cols.append("variant")
        return pd.DataFrame(columns=cols)

    max_event_date = ev["active_date"].max()

    if isinstance(n_days, int):
        n_list = [n_days]
    else:
        n_list = list(n_days)

    cohort_keys = ["signup_date"] + (["variant"] if by_variant and "variant" in us.columns else [])
    cohort_sizes = (
        us.groupby(cohort_keys, as_index=False)["user_id"]
        .nunique()
        .rename(columns={"user_id": "cohort_size"})
    )

    base = us[["user_id", "signup_date"] + (["variant"] if by_variant and "variant" in us.columns else [])].copy()

    rows = []
    for n in n_list:
        tmp = base.copy()
        tmp["target_date"] = (pd.to_datetime(tmp["signup_date"]) + pd.to_timedelta(int(n), unit="D")).dt.date
        tmp["is_observable"] = tmp["target_date"] <= max_event_date

        if drop_incomplete:
            tmp = tmp.loc[tmp["is_observable"]].copy()

        if tmp.empty:
            continue

        join_cols = ["user_id"]
        if by_variant and "variant" in base.columns and "variant" in ev.columns:
            join_cols.append("variant")

        merged = tmp.merge(
            ev,
            how="left",
            left_on=join_cols + ["target_date"],
            right_on=join_cols + ["active_date"],
        )

        merged["_retained"] = (~merged["active_date"].isna()).astype(int)

        grp_cols = ["signup_date"] + (["variant"] if by_variant and "variant" in base.columns else [])
        agg = (
            merged.groupby(grp_cols, as_index=False)["_retained"]
            .sum()
            .rename(columns={"_retained": "retained_users"})
        )
        agg["day_n"] = int(n)

        out = agg.merge(cohort_sizes, on=grp_cols, how="left")
        out["retention_rate"] = out["retained_users"] / out["cohort_size"]
        out = out.rename(columns={"signup_date": "cohort_date"})

        if not drop_incomplete:
            obs = (
                tmp.groupby(grp_cols, as_index=False)["is_observable"]
                .all()
                .rename(columns={"is_observable": "is_fully_observed"})
                .rename(columns={"signup_date": "cohort_date"})
            )
            out = out.merge(
                obs,
                on=["cohort_date"] + (["variant"] if by_variant and "variant" in out.columns else []),
                how="left"
            )

        rows.append(out)

    if not rows:
        cols = ["cohort_date", "day_n", "cohort_size", "retained_users", "retention_rate"]
        if by_variant and "variant" in us.columns:
            cols.append("variant")
        if not drop_incomplete:
            cols.append("is_fully_observed")
        return pd.DataFrame(columns=cols)

    res = pd.concat(rows, ignore_index=True)
    sort_cols = ["cohort_date", "day_n"] + (["variant"] if by_variant and "variant" in us.columns else [])
    return res.sort_values(sort_cols).reset_index(drop=True)


def compute_sessions_per_user(
    sessions: pd.DataFrame,
    events: Optional[pd.DataFrame] = None,
    users: Optional[pd.DataFrame] = None,
    *,
    active_event_names: Optional[Sequence[str]] = None,
    group_cols: Optional[Sequence[str]] = ("variant",),
    active_user_source: str = "events",
) -> pd.DataFrame:
    """
    Sessions per active user, daily:

    sessions_per_user(t) = sessions(t) / DAU(t)

    active_user_source:
    - "events" (recommended): DAU computed from events active_event_names
    - "sessions": active users approximated as unique users with >=1 session that day

    Notes
    -----
    If group_cols include user-level attributes not present on sessions
    (e.g. variant), provide users so sessions can be enriched by user_id.
    """
    se = _prep_sessions(sessions)
    keys = _group_keys(group_cols)

    if keys:
        se = _enrich_with_user_groups(se, users, required_group_cols=keys, df_name="sessions")

    sess_keys = ["session_date"] + keys
    sess_daily = (
        se.groupby(sess_keys, as_index=False)["session_id"]
        .nunique()
        .rename(columns={"session_date": "date", "session_id": "sessions"})
    )
    sess_daily["date"] = pd.to_datetime(sess_daily["date"]).dt.date

    if active_user_source == "events":
        if events is None:
            raise ValueError("events must be provided when active_user_source='events'")
        dau = compute_dau(
            events,
            active_event_names=active_event_names,
            group_cols=keys if keys else None,
        ).rename(columns={"dau": "active_users"})
    elif active_user_source == "sessions":
        au_keys = ["session_date"] + keys
        dau = (
            se.groupby(au_keys, as_index=False)["user_id"]
            .nunique()
            .rename(columns={"session_date": "date", "user_id": "active_users"})
        )
        dau["date"] = pd.to_datetime(dau["date"]).dt.date
    else:
        raise ValueError("active_user_source must be 'events' or 'sessions'")

    merge_keys = ["date"] + keys
    out = sess_daily.merge(dau, on=merge_keys, how="outer").fillna({"sessions": 0, "active_users": 0})

    out["sessions"] = out["sessions"].astype(int)
    out["active_users"] = out["active_users"].astype(int)
    out["sessions_per_user"] = out.apply(
        lambda r: (r["sessions"] / r["active_users"]) if r["active_users"] > 0 else 0.0,
        axis=1,
    )

    return out.sort_values(merge_keys).reset_index(drop=True)


def compute_lesson_funnel(
    events: pd.DataFrame,
    *,
    funnel_steps: Sequence[str] = DEFAULT_FUNNEL_STEPS,
    by: str = "cohort",  # "cohort" or "date"
    users: Optional[pd.DataFrame] = None,  # required if by="cohort"
    group_cols: Optional[Sequence[str]] = ("variant",),
) -> pd.DataFrame:
    """
    Compute a simple lesson funnel.

    Two modes:

    1) by="date":
       For each date (and optional group cols), compute unique users who did each step that day.

    2) by="cohort":
       For each signup cohort (requires users), compute whether each user EVER reached each step,
       and report counts + step conversion rates.

    Returns tidy rows with:
    - grain column: date OR cohort_date
    - step counts: n_signup, n_app_open, n_lesson_started, n_lesson_completed (depending on steps)
    - conversion rates between adjacent steps
    """
    ev = _prep_events(events, active_event_names=None)  # funnel uses explicit names
    _require_cols(ev, ["event_date", "event_name", "user_id"], "events")

    steps = list(funnel_steps)
    keys = _group_keys(group_cols)

    if by not in {"date", "cohort"}:
        raise ValueError("by must be 'date' or 'cohort'")

    if by == "date":
        # Count unique users per date who performed each step
        out_frames = []
        for step in steps:
            sdf = ev.loc[ev["event_name"].eq(step), ["event_date", "user_id"] + [c for c in keys if c in ev.columns]].drop_duplicates()
            sdf = sdf.rename(columns={"event_date": "date"})
            sdf["date"] = pd.to_datetime(sdf["date"]).dt.date
            cnt = sdf.groupby(["date"] + [c for c in keys if c in sdf.columns], as_index=False)["user_id"].nunique()
            cnt = cnt.rename(columns={"user_id": f"n_{step}"})
            out_frames.append(cnt)

        # Outer-merge all step counts
        if not out_frames:
            return pd.DataFrame()

        out = out_frames[0]
        for f in out_frames[1:]:
            merge_keys = ["date"] + [c for c in keys if c in out.columns and c in f.columns]
            out = out.merge(f, on=merge_keys, how="outer")

        out = out.fillna(0)
        # Add conversion rates (adjacent)
        for a, b in zip(steps[:-1], steps[1:]):
            denom = out[f"n_{a}"]
            out[f"cr_{b}_given_{a}"] = out.apply(
                lambda r: (r[f"n_{b}"] / r[f"n_{a}"]) if r[f"n_{a}"] > 0 else 0.0, axis=1
            )

        return out.sort_values(["date"] + [c for c in keys if c in out.columns]).reset_index(drop=True)

    # by == "cohort"
    if users is None:
        raise ValueError("users must be provided when by='cohort'")
    _require_cols(users, ["user_id", "signup_date"], "users")
    us = users.copy()
    us["signup_date"] = _to_date_series(us["signup_date"])

    # Use variant from users as the canonical assignment if available
    cohort_keys = ["signup_date"] + (["variant"] if ("variant" in us.columns and ("variant" in keys or group_cols)) else [])
    cohort_keys = list(dict.fromkeys(cohort_keys))  # unique, preserve order

    # Per-user: did user EVER do step (within data horizon)?
    user_step = (
        ev.loc[ev["event_name"].isin(steps), ["user_id", "event_name"] + [c for c in keys if c in ev.columns]]
        .drop_duplicates()
    )
    # reduce to "user reached step" flags
    user_step["reached"] = 1
    wide = user_step.pivot_table(index=["user_id"] + [c for c in keys if c in user_step.columns],
                                 columns="event_name",
                                 values="reached",
                                 aggfunc="max",
                                 fill_value=0).reset_index()

    # Ensure all steps exist as columns
    for step in steps:
        if step not in wide.columns:
            wide[step] = 0

    # Join to cohorts
    base = us[["user_id", "signup_date"] + ([c for c in keys if c in us.columns] if keys else [])].copy()
    df = base.merge(wide, on=["user_id"] + ([c for c in keys if c in base.columns and c in wide.columns] if keys else []), how="left")
    df[steps] = df[steps].fillna(0).astype(int)

    # signup step: by definition every cohort member is "signed up"
    df["signup"] = 1 if "signup" in steps else df.get("signup", 1)

    # Aggregate
    agg = df.groupby(cohort_keys, as_index=False)[steps].sum()
    # Rename counts
    for step in steps:
        agg = agg.rename(columns={step: f"n_{step}"})

    # Conversion rates adjacent
    for a, b in zip(steps[:-1], steps[1:]):
        agg[f"cr_{b}_given_{a}"] = agg.apply(
            lambda r: (r[f"n_{b}"] / r[f"n_{a}"]) if r[f"n_{a}"] > 0 else 0.0, axis=1
        )

    agg = agg.rename(columns={"signup_date": "cohort_date"})
    return agg.sort_values(["cohort_date"] + ([c for c in cohort_keys if c != "signup_date"])).reset_index(drop=True)


# -----------------------
# Growth-model lifecycle (optional)
# -----------------------

LIFECYCLE_STATES = [
    "Reactivated",
    "New",
    "Resurrected",
    "Current",
    "AtRiskWAU",
    "AtRiskMAU",
    "Dormant",
]


def compute_lifecycle_counts(
    events: pd.DataFrame,
    users: pd.DataFrame,
    *,
    active_event_names: Optional[Sequence[str]] = None,
    group_cols: Optional[Sequence[str]] = ("variant",),
    date_min: Optional[str] = None,
    date_max: Optional[str] = None,
) -> pd.DataFrame:
    """
    Compute daily lifecycle counts by state using window definitions:

    - New: date == signup_date
    - Current: active today AND active in last 7 days (excluding today) >= 1
    - Reactivated: active today AND no activity in last 7 days (excluding today)
      AND activity in last 30 days (excluding today) >= 1
    - Resurrected: active today AND no activity in last 30 days (excluding today)
    - AtRiskWAU: inactive today AND activity in last 7 days (including today) >= 1
    - AtRiskMAU: no activity in last 7 days AND activity in last 30 days >= 1
    - Dormant: no activity in last 30 days

    Output columns:
    date, state, users, [group cols]
    """
    _require_cols(users, ["user_id", "signup_date"], "users")
    us = users.copy()
    us["signup_date"] = _to_date_series(us["signup_date"])

    keys = _group_keys(group_cols)
    if keys:
        _require_cols(us, ["user_id", "signup_date"] + keys, "users")

    ev = _prep_events(events, active_event_names=active_event_names)

    # Attach grouping columns to events if they are not already present there
    if keys:
        ev = _enrich_with_user_groups(ev, us, required_group_cols=keys, df_name="events")

    # Unique active user-days
    ud = ev.loc[
        ev["_is_active_event"],
        ["user_id", "event_date"] + keys
    ].drop_duplicates()

    ud = ud.rename(columns={"event_date": "date"})
    if not ud.empty:
        ud["date"] = pd.to_datetime(ud["date"]).dt.date

    # Determine overall computation range
    signup_min = us["signup_date"].min()
    signup_max = us["signup_date"].max()
    active_min = ud["date"].min() if not ud.empty else signup_min
    active_max = ud["date"].max() if not ud.empty else signup_max

    d0 = pd.to_datetime(date_min).date() if date_min else min(signup_min, active_min)
    d1 = pd.to_datetime(date_max).date() if date_max else max(signup_max, active_max)

    if d1 < d0:
        raise ValueError(f"Invalid date range: date_min={d0}, date_max={d1}")

    all_dates = pd.date_range(d0, d1, freq="D").date

    rows = []

    if keys:
        user_groups_iter = us.groupby(keys, dropna=False)
    else:
        user_groups_iter = [(None, us)]

    for gvals, ugrp in user_groups_iter:
        ugrp = ugrp.copy().sort_values(["user_id", "signup_date"])
        signup_map = dict(zip(ugrp["user_id"], ugrp["signup_date"]))
        group_user_ids = list(signup_map.keys())

        # active dates restricted to this group
        if not ud.empty:
            if keys:
                gdf = ud.copy()
                gtuple = gvals if isinstance(gvals, tuple) else (gvals,)
                for k, v in zip(keys, gtuple):
                    gdf = gdf.loc[gdf[k].eq(v)]
            else:
                gdf = ud
            active_dates_by_user = gdf.groupby("user_id")["date"].apply(set).to_dict() if not gdf.empty else {}
        else:
            active_dates_by_user = {}

        for t in all_dates:
            counts = {s: 0 for s in LIFECYCLE_STATES}

            t7_prev = set(
                pd.date_range(
                    pd.to_datetime(t) - pd.Timedelta(days=7),
                    pd.to_datetime(t) - pd.Timedelta(days=1),
                    freq="D",
                ).date
            )
            t30_prev = set(
                pd.date_range(
                    pd.to_datetime(t) - pd.Timedelta(days=30),
                    pd.to_datetime(t) - pd.Timedelta(days=1),
                    freq="D",
                ).date
            )
            t7_incl = set(
                pd.date_range(
                    pd.to_datetime(t) - pd.Timedelta(days=6),
                    pd.to_datetime(t),
                    freq="D",
                ).date
            )
            t30_incl = set(
                pd.date_range(
                    pd.to_datetime(t) - pd.Timedelta(days=29),
                    pd.to_datetime(t),
                    freq="D",
                ).date
            )

            for uid in group_user_ids:
                sdate = signup_map[uid]
                if sdate > t:
                    continue

                act_set = active_dates_by_user.get(uid, set())

                # New takes precedence on signup day
                if sdate == t:
                    counts["New"] += 1
                    continue

                A_today = 1 if t in act_set else 0
                w7_prev = len(act_set & t7_prev)
                w30_prev = len(act_set & t30_prev)
                w7_incl = len(act_set & t7_incl)
                w30_incl = len(act_set & t30_incl)

                if A_today == 1:
                    if w7_prev >= 1:
                        counts["Current"] += 1
                    elif w30_prev >= 1:
                        counts["Reactivated"] += 1
                    else:
                        counts["Resurrected"] += 1
                else:
                    if w7_incl >= 1:
                        counts["AtRiskWAU"] += 1
                    elif w30_incl >= 1:
                        counts["AtRiskMAU"] += 1
                    else:
                        counts["Dormant"] += 1

            for state, n in counts.items():
                row = {"date": t, "state": state, "users": int(n)}
                if keys:
                    gtuple = gvals if isinstance(gvals, tuple) else (gvals,)
                    row.update({k: v for k, v in zip(keys, gtuple)})
                rows.append(row)

    out = pd.DataFrame(rows)
    sort_cols = ["date"] + keys + ["state"]
    return out.sort_values(sort_cols).reset_index(drop=True)



def _safe_divide(numer: pd.Series, denom: pd.Series) -> pd.Series:
    numer = numer.astype(float)
    denom = denom.astype(float)
    out = numer / denom.where(denom != 0)
    return out.fillna(0.0)


def _build_user_date_spine(
    users: pd.DataFrame,
    *,
    date_min: Optional[str] = None,
    date_max: Optional[str] = None,
) -> pd.DataFrame:
    """
    Build 1 row per signed-up user per date, starting from signup_date.
    """
    _require_cols(users, ["user_id", "signup_date"], "users")
    us = users.copy()
    us["signup_date"] = _to_date_series(us["signup_date"])

    global_min = pd.to_datetime(date_min).date() if date_min else us["signup_date"].min()
    global_max = pd.to_datetime(date_max).date() if date_max else us["signup_date"].max()

    if global_max < global_min:
        raise ValueError(f"Invalid date range: {global_min} > {global_max}")

    rows = []
    for _, r in us.iterrows():
        sdate = r["signup_date"]
        start = max(sdate, global_min)
        if start > global_max:
            continue
        for d in pd.date_range(start, global_max, freq="D").date:
            rows.append({
                "date": d,
                "user_id": r["user_id"],
                "signup_date": sdate,
            })

    spine = pd.DataFrame(rows)
    if spine.empty:
        return pd.DataFrame(columns=["date", "user_id", "signup_date", "days_since_signup"])

    spine["days_since_signup"] = (
        pd.to_datetime(spine["date"]) - pd.to_datetime(spine["signup_date"])
    ).dt.days.astype(int)

    return spine


def build_fact_user_daily(
    users: pd.DataFrame,
    events: pd.DataFrame,
    sessions: pd.DataFrame,
    *,
    active_event_names: Optional[Sequence[str]] = None,
    date_min: Optional[str] = None,
    date_max: Optional[str] = None,
    include_lifecycle: bool = True,
    sample_n_users: Optional[int] = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Build fact_user_daily with grain = 1 user x 1 date.

    Improvements in this version
    ----------------------------
    - prints progress / heartbeats inside the function
    - supports sample_n_users for faster debugging
    - supports include_lifecycle=False to isolate bottlenecks
    - avoids some unnecessary merges / repeated conversions
    """

    def log(msg: str) -> None:
        if verbose:
            print(f"[fact_user_daily] {msg}", flush=True)

    t0 = time.time()
    log("START")

    _require_cols(users, ["user_id", "signup_date"], "users")

    # -----------------------
    # 1) Prepare users
    # -----------------------
    t = time.time()
    us = users.copy()
    us["signup_date"] = _to_date_series(us["signup_date"])

    if sample_n_users is not None:
        if sample_n_users <= 0:
            raise ValueError("sample_n_users must be positive when provided")
        sample_n = min(sample_n_users, len(us))
        us = us.sort_values("user_id").head(sample_n).copy()
        keep_user_ids = set(us["user_id"].tolist())
        events = events.loc[events["user_id"].isin(keep_user_ids)].copy()
        sessions = sessions.loc[sessions["user_id"].isin(keep_user_ids)].copy()
        log(f"sample mode enabled: users={len(us):,}, events={len(events):,}, sessions={len(sessions):,}")

    user_cols = ["user_id", "signup_date"]
    for c in ["variant", "country", "signup_channel", "language_target", "device_os", "app_version"]:
        if c in us.columns:
            user_cols.append(c)
    if "is_premium_at_signup" in us.columns:
        user_cols.append("is_premium_at_signup")

    uattrs = us[user_cols].drop_duplicates(subset=["user_id"]).copy()
    log(f"1/8 users prepared | shape={uattrs.shape[0]:,}x{uattrs.shape[1]} | {time.time()-t:,.1f}s")

    # -----------------------
    # 2) Prepare events / sessions
    # -----------------------
    t = time.time()
    ev = _prep_events(events, active_event_names=active_event_names)
    se = _prep_sessions(sessions)

    # reduce to only relevant users
    valid_user_ids = set(uattrs["user_id"].tolist())
    ev = ev.loc[ev["user_id"].isin(valid_user_ids)].copy()
    se = se.loc[se["user_id"].isin(valid_user_ids)].copy()

    # normalize dates once
    if not ev.empty:
        ev["event_date"] = pd.to_datetime(ev["event_date"]).dt.date
    if not se.empty:
        se["session_date"] = pd.to_datetime(se["session_date"]).dt.date

    inferred_max_candidates = [us["signup_date"].max()]
    if not ev.empty:
        inferred_max_candidates.append(ev["event_date"].max())
    if not se.empty:
        inferred_max_candidates.append(se["session_date"].max())
    inferred_max = max(inferred_max_candidates)

    final_date_max = date_max if date_max is not None else str(inferred_max)

    log(
        f"2/8 raw prepared | "
        f"users={len(uattrs):,}, events={len(ev):,}, sessions={len(se):,}, "
        f"date_max={final_date_max} | {time.time()-t:,.1f}s"
    )

    # -----------------------
    # 3) Build user-date spine
    # -----------------------
    t = time.time()
    spine = _build_user_date_spine(
        us[["user_id", "signup_date"]].copy(),
        date_min=date_min,
        date_max=final_date_max,
    )

    if spine.empty:
        log("3/8 spine empty -> returning empty fact_user_daily")
        return pd.DataFrame(columns=[
            "date", "user_id", "variant", "signup_date", "days_since_signup",
            "is_active", "had_app_open", "had_lesson_started", "had_lesson_completed",
            "sessions_count", "session_duration_sec", "lessons_completed", "xp_earned",
            "hearts_lost", "streak_length_end_of_day", "is_premium", "lifecycle_state"
        ])

    fact = spine.merge(uattrs, on=["user_id", "signup_date"], how="left")
    fact["date"] = pd.to_datetime(fact["date"]).dt.date
    log(f"3/8 spine built | shape={fact.shape[0]:,}x{fact.shape[1]} | {time.time()-t:,.1f}s")

    # -----------------------
    # 4) Aggregate event-derived daily features
    # -----------------------
    t = time.time()
    if ev.empty:
        active_daily = pd.DataFrame(columns=["user_id", "date", "is_active"])
        step_flags = pd.DataFrame(columns=["user_id", "date", "had_app_open", "had_lesson_started", "had_lesson_completed"])
        event_rollup = pd.DataFrame(columns=["user_id", "date", "lessons_completed", "xp_earned", "hearts_lost"])
        latest_daily = pd.DataFrame(columns=["user_id", "date", "streak_length_end_of_day", "is_premium"])
    else:
        ev_daily = ev.copy()
        ev_daily = ev_daily.rename(columns={"event_date": "date"})

        active_daily = (
            ev_daily.groupby(["user_id", "date"], as_index=False)["_is_active_event"]
            .max()
            .rename(columns={"_is_active_event": "is_active"})
        )

        step_flags = (
            ev_daily.assign(
                had_app_open=ev_daily["event_name"].eq("app_open").astype("int8"),
                had_lesson_started=ev_daily["event_name"].eq("lesson_started").astype("int8"),
                had_lesson_completed=ev_daily["event_name"].eq("lesson_completed").astype("int8"),
            )
            .groupby(["user_id", "date"], as_index=False)[
                ["had_app_open", "had_lesson_started", "had_lesson_completed"]
            ]
            .max()
        )

        event_rollup = (
            ev_daily.assign(
                lessons_completed=ev_daily["event_name"].eq("lesson_completed").astype("int16"),
                hearts_lost=(-ev_daily["hearts_delta"]).clip(lower=0),
            )
            .groupby(["user_id", "date"], as_index=False)[["lessons_completed", "xp_delta", "hearts_lost"]]
            .sum()
            .rename(columns={"xp_delta": "xp_earned"})
        )

        # latest event of user-date
        if "event_time" in ev_daily.columns:
            ev_daily["event_time"] = pd.to_datetime(ev_daily["event_time"])
            latest_daily = (
                ev_daily.sort_values(["user_id", "date", "event_time"])
                .groupby(["user_id", "date"], as_index=False)
                .tail(1)[["user_id", "date", "streak_length", "is_premium"]]
                .rename(columns={"streak_length": "streak_length_end_of_day"})
            )
        else:
            latest_daily = (
                ev_daily.groupby(["user_id", "date"], as_index=False)[["streak_length", "is_premium"]]
                .last()
                .rename(columns={"streak_length": "streak_length_end_of_day"})
            )

    log(
        f"4/8 event daily aggregates built | "
        f"active={len(active_daily):,}, steps={len(step_flags):,}, rollup={len(event_rollup):,}, latest={len(latest_daily):,} "
        f"| {time.time()-t:,.1f}s"
    )

    # -----------------------
    # 5) Aggregate session-derived daily features
    # -----------------------
    t = time.time()
    if se.empty:
        session_rollup = pd.DataFrame(columns=["user_id", "date", "sessions_count", "session_duration_sec"])
    else:
        se_daily = se.rename(columns={"session_date": "date"}).copy()
        session_rollup = (
            se_daily.groupby(["user_id", "date"], as_index=False)
            .agg(
                sessions_count=("session_id", "nunique"),
                session_duration_sec=("session_duration_sec", "sum"),
            )
        )

    log(f"5/8 session daily aggregates built | rows={len(session_rollup):,} | {time.time()-t:,.1f}s")

    # -----------------------
    # 6) Merge all daily features onto spine
    # -----------------------
    t = time.time()
    for name, df in [
        ("active_daily", active_daily),
        ("step_flags", step_flags),
        ("event_rollup", event_rollup),
        ("latest_daily", latest_daily),
        ("session_rollup", session_rollup),
    ]:
        if not df.empty:
            fact = fact.merge(df, on=["user_id", "date"], how="left")
            log(f"6/8 merged {name:<14} -> fact shape={fact.shape[0]:,}x{fact.shape[1]}")
        else:
            log(f"6/8 skipped empty {name}")

    # fill defaults
    for c in ["is_active", "had_app_open", "had_lesson_started", "had_lesson_completed"]:
        if c not in fact.columns:
            fact[c] = 0
        fact[c] = fact[c].fillna(0).astype("int8")

    for c in [
        "sessions_count",
        "session_duration_sec",
        "lessons_completed",
        "xp_earned",
        "hearts_lost",
        "streak_length_end_of_day",
    ]:
        if c not in fact.columns:
            fact[c] = 0
        fact[c] = fact[c].fillna(0).astype("int32")

    # premium fallback for days without events
    if "is_premium" not in fact.columns:
        fact["is_premium"] = False
    else:
        if "is_premium_at_signup" in fact.columns:
            fact["is_premium"] = fact["is_premium"].fillna(fact["is_premium_at_signup"]).fillna(False)
        else:
            fact["is_premium"] = fact["is_premium"].fillna(False)

    log(f"6/8 merge + fill complete | shape={fact.shape[0]:,}x{fact.shape[1]} | {time.time()-t:,.1f}s")

    # -----------------------
    # 7) Lifecycle assignment
    # -----------------------
    t = time.time()
    if include_lifecycle:
        log("7/8 assigning lifecycle_state ... this may take a while")
        fact = _assign_lifecycle_state_from_fact_with_progress(fact, verbose=verbose)
    else:
        fact["lifecycle_state"] = None
        log("7/8 lifecycle skipped")

    log(f"7/8 lifecycle complete | {time.time()-t:,.1f}s")

    # -----------------------
    # 8) Final column selection
    # -----------------------
    t = time.time()
    wanted = [
        "date", "user_id", "variant", "signup_date", "days_since_signup",
        "is_active", "had_app_open", "had_lesson_started", "had_lesson_completed",
        "sessions_count", "session_duration_sec", "lessons_completed", "xp_earned",
        "hearts_lost", "streak_length_end_of_day", "is_premium", "lifecycle_state"
    ]
    existing = [c for c in wanted if c in fact.columns]
    fact = fact[existing].sort_values(["date", "user_id"]).reset_index(drop=True)

    log(f"8/8 final table ready | shape={fact.shape[0]:,}x{fact.shape[1]} | {time.time()-t:,.1f}s")
    log(f"DONE | total {time.time()-t0:,.1f}s")

    return fact


def _assign_lifecycle_state_from_fact(fact_user_daily: pd.DataFrame) -> pd.DataFrame:
    """
    Assign user-day lifecycle_state directly from fact_user_daily.
    """
    _require_cols(
        fact_user_daily,
        ["date", "user_id", "signup_date", "is_active"],
        "fact_user_daily",
    )
    f = fact_user_daily.copy()
    f["date"] = _to_date_series(f["date"])
    f["signup_date"] = _to_date_series(f["signup_date"])
    f = f.sort_values(["user_id", "date"]).reset_index(drop=True)

    out_states = []

    for uid, g in f.groupby("user_id", sort=False):
        g = g.sort_values("date").copy()
        active_dates = set(g.loc[g["is_active"].astype(bool), "date"])

        states = []
        for _, r in g.iterrows():
            t = r["date"]
            sdate = r["signup_date"]

            if t == sdate:
                states.append("New")
                continue

            t7_prev = set(pd.date_range(pd.to_datetime(t) - pd.Timedelta(days=7),
                                        pd.to_datetime(t) - pd.Timedelta(days=1), freq="D").date)
            t30_prev = set(pd.date_range(pd.to_datetime(t) - pd.Timedelta(days=30),
                                         pd.to_datetime(t) - pd.Timedelta(days=1), freq="D").date)
            t7_incl = set(pd.date_range(pd.to_datetime(t) - pd.Timedelta(days=6),
                                        pd.to_datetime(t), freq="D").date)
            t30_incl = set(pd.date_range(pd.to_datetime(t) - pd.Timedelta(days=29),
                                         pd.to_datetime(t), freq="D").date)

            A_today = t in active_dates
            w7_prev = len(active_dates & t7_prev)
            w30_prev = len(active_dates & t30_prev)
            w7_incl = len(active_dates & t7_incl)
            w30_incl = len(active_dates & t30_incl)

            if A_today:
                if w7_prev >= 1:
                    states.append("Current")
                elif w30_prev >= 1:
                    states.append("Reactivated")
                else:
                    states.append("Resurrected")
            else:
                if w7_incl >= 1:
                    states.append("AtRiskWAU")
                elif w30_incl >= 1:
                    states.append("AtRiskMAU")
                else:
                    states.append("Dormant")

        g["lifecycle_state"] = states
        out_states.append(g)

    return pd.concat(out_states, ignore_index=True)


def build_agg_daily_kpis(
    fact_user_daily: pd.DataFrame,
    users: pd.DataFrame,
    events: pd.DataFrame,
    *,
    group_cols: Optional[Sequence[str]] = ("variant",),
    active_event_names: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """
    Build agg_daily_kpis at grain = date x group_cols.
    """
    keys = _group_keys(group_cols)
    f = fact_user_daily.copy()
    f["date"] = _to_date_series(f["date"])

    # WAU/MAU from canonical event definition
    dau = compute_dau(events, active_event_names=active_event_names, group_cols=keys if keys else None)
    wau = compute_wau(events, active_event_names=active_event_names, group_cols=keys if keys else None)
    mau = compute_mau(events, active_event_names=active_event_names, group_cols=keys if keys else None)

    # signups from users
    us = users.copy()
    us["signup_date"] = _to_date_series(us["signup_date"])
    signup_group_cols = ["signup_date"] + [c for c in keys if c in us.columns]
    signups = (
        us.groupby(signup_group_cols, as_index=False)["user_id"]
        .nunique()
        .rename(columns={"signup_date": "date", "user_id": "signups"})
    )

    agg = (
        f.groupby(["date"] + keys, as_index=False)
        .agg(
            sessions=("sessions_count", "sum"),
            avg_session_duration_sec=("session_duration_sec", "mean"),
            lessons_completed=("lessons_completed", "sum"),
            xp_earned=("xp_earned", "sum"),
            premium_users=("is_premium", "sum"),
            active_users=("is_active", "sum"),
        )
    )

    # use sessions_count sum / active_users
    agg["sessions_per_active_user"] = _safe_divide(agg["sessions"], agg["active_users"])
    agg["lessons_per_active_user"] = _safe_divide(agg["lessons_completed"], agg["active_users"])
    agg["xp_per_active_user"] = _safe_divide(agg["xp_earned"], agg["active_users"])
    agg["premium_share"] = _safe_divide(agg["premium_users"], agg["active_users"])

    out = agg.merge(signups, on=["date"] + [c for c in keys if c in signups.columns], how="left")
    out = out.merge(dau, on=["date"] + [c for c in keys if c in dau.columns], how="left")
    out = out.merge(wau, on=["date"] + [c for c in keys if c in wau.columns], how="left")
    out = out.merge(mau, on=["date"] + [c for c in keys if c in mau.columns], how="left")

    for c in ["signups", "dau", "wau", "mau"]:
        if c not in out.columns:
            out[c] = 0
        out[c] = out[c].fillna(0)

    out["dau_mau_ratio"] = _safe_divide(out["dau"], out["mau"])

    wanted = [
        "date", *keys, "signups", "dau", "wau", "mau", "dau_mau_ratio",
        "sessions", "avg_session_duration_sec", "sessions_per_active_user",
        "lessons_completed", "lessons_per_active_user", "xp_earned",
        "xp_per_active_user", "premium_users", "premium_share"
    ]
    wanted = [c for c in wanted if c in out.columns]
    return out[wanted].sort_values(["date"] + keys).reset_index(drop=True)


def build_agg_retention_cohort(
    users: pd.DataFrame,
    events: pd.DataFrame,
    *,
    n_days: Union[int, Sequence[int]] = (1, 7, 30),
    active_event_names: Optional[Sequence[str]] = None,
    by_variant: bool = True,
) -> pd.DataFrame:
    """
    Build agg_retention_cohort directly from canonical retention definition.
    """
    return compute_retention(
        users=users,
        events=events,
        n_days=n_days,
        active_event_names=active_event_names,
        by_variant=by_variant,
        drop_incomplete=True,
    ).reset_index(drop=True)


def build_agg_funnel_daily(
    events: pd.DataFrame,
    *,
    group_cols: Optional[Sequence[str]] = ("variant",),
    funnel_steps: Sequence[str] = DEFAULT_FUNNEL_STEPS,
) -> pd.DataFrame:
    """
    Build agg_funnel_daily at grain = date x group_cols.
    """
    return compute_lesson_funnel(
        events=events,
        funnel_steps=funnel_steps,
        by="date",
        users=None,
        group_cols=group_cols,
    ).reset_index(drop=True)


def build_agg_lifecycle_daily(
    fact_user_daily: pd.DataFrame,
    *,
    group_cols: Optional[Sequence[str]] = ("variant",),
) -> pd.DataFrame:
    """
    Build agg_lifecycle_daily at grain = date x state x group_cols.
    """
    keys = _group_keys(group_cols)
    f = fact_user_daily.copy()
    f["date"] = _to_date_series(f["date"])

    out = (
        f.groupby(["date", "lifecycle_state"] + keys, as_index=False)["user_id"]
        .nunique()
        .rename(columns={"lifecycle_state": "state", "user_id": "users"})
    )
    return out.sort_values(["date", "state"] + keys).reset_index(drop=True)


def build_dashboard_tables(
    users: pd.DataFrame,
    events: pd.DataFrame,
    sessions: pd.DataFrame,
    *,
    active_event_names: Optional[Sequence[str]] = None,
    retention_days: Union[int, Sequence[int]] = (1, 7, 30),
    group_cols: Optional[Sequence[str]] = ("variant",),
    date_min: Optional[str] = None,
    date_max: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Convenience wrapper to produce all recommended dashboard-serving tables.
    """
    fact_user_daily = build_fact_user_daily(
        users=users,
        events=events,
        sessions=sessions,
        active_event_names=active_event_names,
        date_min=date_min,
        date_max=date_max,
    )

    agg_daily_kpis = build_agg_daily_kpis(
        fact_user_daily=fact_user_daily,
        users=users,
        events=events,
        group_cols=group_cols,
        active_event_names=active_event_names,
    )

    agg_retention_cohort = build_agg_retention_cohort(
        users=users,
        events=events,
        n_days=retention_days,
        active_event_names=active_event_names,
        by_variant=("variant" in _group_keys(group_cols)),
    )

    agg_funnel_daily = build_agg_funnel_daily(
        events=events,
        group_cols=group_cols,
        funnel_steps=DEFAULT_FUNNEL_STEPS,
    )

    agg_lifecycle_daily = build_agg_lifecycle_daily(
        fact_user_daily=fact_user_daily,
        group_cols=group_cols,
    )

    return {
        "fact_user_daily": fact_user_daily,
        "agg_daily_kpis": agg_daily_kpis,
        "agg_retention_cohort": agg_retention_cohort,
        "agg_funnel_daily": agg_funnel_daily,
        "agg_lifecycle_daily": agg_lifecycle_daily,
    }