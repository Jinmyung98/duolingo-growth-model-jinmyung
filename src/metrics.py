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
from typing import Iterable, Optional, Sequence, Union, List

import pandas as pd


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
) -> pd.DataFrame:
    """
    Cohort retention:
    For cohort c (signup_date = c), user is retained on day n if active on (c + n).

    Output (long-form):
    cohort_date, day_n, cohort_size, retained_users, retention_rate, [variant]
    """
    _require_cols(users, ["user_id", "signup_date"], "users")
    us = users.copy()
    us["signup_date"] = _to_date_series(us["signup_date"])

    ev = _prep_events(events, active_event_names=active_event_names)
    ev = ev.loc[ev["_is_active_event"], ["user_id", "event_date"] + (["variant"] if "variant" in ev.columns else [])].drop_duplicates()
    ev = ev.rename(columns={"event_date": "active_date"})
    ev["active_date"] = pd.to_datetime(ev["active_date"]).dt.date

    if isinstance(n_days, int):
        n_list = [n_days]
    else:
        n_list = list(n_days)

    # Cohort size
    cohort_keys = ["signup_date"] + (["variant"] if by_variant and "variant" in us.columns else [])
    cohort_sizes = us.groupby(cohort_keys, as_index=False)["user_id"].nunique().rename(columns={"user_id": "cohort_size"})

    # Join users to activity
    base = us[["user_id", "signup_date"] + (["variant"] if by_variant and "variant" in us.columns else [])].copy()

    rows = []
    for n in n_list:
        tmp = base.copy()
        tmp["target_date"] = (pd.to_datetime(tmp["signup_date"]) + pd.to_timedelta(n, unit="D")).dt.date

        # Determine whether user active on target_date
        join_cols = ["user_id"]
        if by_variant and "variant" in base.columns and "variant" in ev.columns:
            join_cols.append("variant")

        merged = tmp.merge(
            ev,
            how="left",
            left_on=join_cols + ["target_date"],
            right_on=join_cols + ["active_date"],
            indicator=False,
        )

        merged["_retained"] = (~merged["active_date"].isna()).astype(int)

        grp_cols = ["signup_date"] + (["variant"] if by_variant and "variant" in base.columns else [])
        agg = merged.groupby(grp_cols, as_index=False)["_retained"].sum().rename(columns={"_retained": "retained_users"})
        agg["day_n"] = int(n)

        # attach cohort size
        out = agg.merge(cohort_sizes, on=grp_cols, how="left")
        out["retention_rate"] = out["retained_users"] / out["cohort_size"]
        out = out.rename(columns={"signup_date": "cohort_date"})
        rows.append(out)

    res = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    sort_cols = ["cohort_date", "day_n"] + (["variant"] if by_variant and "variant" in us.columns else [])
    return res.sort_values(sort_cols).reset_index(drop=True)


def compute_sessions_per_user(
    sessions: pd.DataFrame,
    events: Optional[pd.DataFrame] = None,
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
    """
    se = _prep_sessions(sessions)
    keys = _group_keys(group_cols)

    # sessions per day
    sess_keys = ["session_date"] + [c for c in keys if c in se.columns]
    sess_daily = se.groupby(sess_keys, as_index=False)["session_id"].nunique().rename(
        columns={"session_date": "date", "session_id": "sessions"}
    )
    sess_daily["date"] = pd.to_datetime(sess_daily["date"]).dt.date

    if active_user_source == "events":
        if events is None:
            raise ValueError("events must be provided when active_user_source='events'")
        dau = compute_dau(events, active_event_names=active_event_names, group_cols=keys if keys else None)
        dau = dau.rename(columns={"dau": "active_users"})
    elif active_user_source == "sessions":
        # Approx: unique users with sessions that day
        au_keys = ["session_date"] + [c for c in keys if c in se.columns]
        dau = (
            se.groupby(au_keys, as_index=False)["user_id"]
            .nunique()
            .rename(columns={"session_date": "date", "user_id": "active_users"})
        )
        dau["date"] = pd.to_datetime(dau["date"]).dt.date
    else:
        raise ValueError("active_user_source must be 'events' or 'sessions'")

    # merge
    merge_keys = ["date"] + [c for c in keys if c in sess_daily.columns and c in dau.columns]
    out = sess_daily.merge(dau, on=merge_keys, how="outer").fillna({"sessions": 0, "active_users": 0})
    out["sessions_per_user"] = out.apply(
        lambda r: (r["sessions"] / r["active_users"]) if r["active_users"] > 0 else 0.0,
        axis=1
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
    users: Optional[pd.DataFrame] = None,
    *,
    active_event_names: Optional[Sequence[str]] = None,
    group_cols: Optional[Sequence[str]] = ("variant",),
    date_min: Optional[str] = None,
    date_max: Optional[str] = None,
) -> pd.DataFrame:
    """
    Compute daily lifecycle counts by state using window definitions:

    - New: date == signup_date (requires users)
    - Current: active today AND active in last 7 days (excluding today) >= 1
    - Reactivated: active today AND no activity in last 7 days (excluding today) AND activity in last 30 days (excluding today) >= 1
    - Resurrected: active today AND no activity in last 30 days (excluding today)
    - AtRiskWAU: inactive today AND activity in last 7 days (including today) >= 1
    - AtRiskMAU: no activity in last 7 days AND activity in last 30 days >= 1
    - Dormant: no activity in last 30 days

    Notes:
    - This is path/window-based, so it needs per-user daily activity series.
    - For large datasets, consider materializing active user-days first.

    Output columns:
    date, state, users, [group cols]
    """
    ev = _prep_events(events, active_event_names=active_event_names)
    keys = _group_keys(group_cols)

    # Build unique active user-days
    ud = ev.loc[ev["_is_active_event"], ["user_id", "event_date"] + [c for c in keys if c in ev.columns]].drop_duplicates()
    ud = ud.rename(columns={"event_date": "date"})
    ud["date"] = pd.to_datetime(ud["date"]).dt.date

    if ud.empty:
        cols = ["date"] + keys + ["state", "users"]
        return pd.DataFrame(columns=cols)

    # Determine date range to compute over
    d0 = pd.to_datetime(date_min).date() if date_min else ud["date"].min()
    d1 = pd.to_datetime(date_max).date() if date_max else ud["date"].max()
    all_dates = pd.date_range(d0, d1, freq="D").date

    # Signup dates (for "New")
    signup = None
    if users is not None:
        _require_cols(users, ["user_id", "signup_date"], "users")
        signup = users[["user_id", "signup_date"] + ([c for c in keys if c in users.columns] if keys else [])].copy()
        signup["signup_date"] = _to_date_series(signup["signup_date"])

    rows = []
    for gvals, gdf in ud.groupby(keys, dropna=False) if keys else [(None, ud)]:
        gdf = gdf.sort_values(["user_id", "date"])
        # user -> set(active_dates)
        active_dates_by_user = gdf.groupby("user_id")["date"].apply(set).to_dict()

        # optional signup map
        signup_map = {}
        if signup is not None:
            ssub = signup
            if keys:
                if not isinstance(gvals, tuple):
                    gvals = (gvals,)
                for k, v in zip(keys, gvals):
                    if k in ssub.columns:
                        ssub = ssub.loc[ssub[k].eq(v)]
            signup_map = dict(zip(ssub["user_id"], ssub["signup_date"]))

        for t in all_dates:
            # For each user, compute A_t and window counts (simple set logic)
            # This is not the most optimized approach but works for moderate synthetic data.
            counts = {s: 0 for s in LIFECYCLE_STATES}

            t7_prev = set(pd.date_range(pd.to_datetime(t) - pd.Timedelta(days=7), pd.to_datetime(t) - pd.Timedelta(days=1), freq="D").date)
            t30_prev = set(pd.date_range(pd.to_datetime(t) - pd.Timedelta(days=30), pd.to_datetime(t) - pd.Timedelta(days=1), freq="D").date)
            t7_incl = set(pd.date_range(pd.to_datetime(t) - pd.Timedelta(days=6), pd.to_datetime(t), freq="D").date)
            t30_incl = set(pd.date_range(pd.to_datetime(t) - pd.Timedelta(days=29), pd.to_datetime(t), freq="D").date)

            for uid, act_set in active_dates_by_user.items():
                A_today = 1 if t in act_set else 0
                w7_prev = len(act_set & t7_prev)
                w30_prev = len(act_set & t30_prev)
                w7_incl = len(act_set & t7_incl)
                w30_incl = len(act_set & t30_incl)

                # New
                if uid in signup_map and signup_map[uid] == t:
                    counts["New"] += 1
                    continue

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
                row = {"date": t, "state": state, "users": n}
                if keys:
                    if not isinstance(gvals, tuple):
                        gvals = (gvals,)
                    row.update({k: v for k, v in zip(keys, gvals)})
                rows.append(row)

    out = pd.DataFrame(rows)
    return out.sort_values(["date"] + keys + ["state"]).reset_index(drop=True)