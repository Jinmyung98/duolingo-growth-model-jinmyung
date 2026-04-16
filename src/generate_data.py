# generate_data.py
# Duolingo-style Growth Model Simulation + Rich Analytics Schema (Phase 1)
#
# Outputs:
#   data/users.csv
#   data/sessions.csv
#   data/events.csv
#   data/lessons.csv
#
# Run:
#   python generate_data.py --n_users 50000 --seed 42 --out_dir data
#
# Notes:
# - Daily granularity over n_days (default 365).
# - Each user has a lifecycle state S_t (7-state Markov) and an activity/streak process.
# - Sessions + event stream are generated only on active days.
# - Treatment affects (a) transition matrices on selected rows and (b) streak sensitivity after rollout.
#
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

import time
import sys

# -------------------------
# 0) Constants & helpers
# -------------------------
STATES: Dict[int, str] = {
    1: "Reactivated Users",
    2: "New Users",
    3: "Resurrected Users",
    4: "Current Users",
    5: "At Risk WAUs",
    6: "At Risk MAUs",
    7: "Dormant Users",
}
STATE_IDS = list(STATES.keys())
IDX = {sid: i for i, sid in enumerate(STATE_IDS)}  # state_id -> row/col index

EVENT_TAXONOMY = {
    # Acquisition & Onboarding
    "signup",
    "onboarding_completed",
    "paywall_shown",
    "purchase",
    # Core engagement
    "app_open",
    "app_background",
    "push_received",
    "push_opened",
    # Learning flow
    "lesson_started",
    "question_answered",
    "lesson_completed",
    # Habit mechanics
    "streak_incremented",
    "streak_broken",
    "streak_repaired",
}

SCREENS = [
    "onboarding", "home", "lesson", "lesson_result", "shop"
]

COUNTRIES = ["AU", "US", "CN"]
LANG_TARGETS = ["EN", "ES", "JA"]
DEVICE_OS = ["iOS", "Android", "Web"]
SIGNUP_PLATFORM = ["mobile", "web"]
TIMEZONES = ["Australia/Perth"]
APP_VERSIONS = ["1.12.3", "1.12.4", "1.12.5"]


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def clamp01(x: float, lo: float = 1e-6, hi: float = 1 - 1e-6) -> float:
    return float(np.clip(x, lo, hi))


def sigmoid(x: float) -> float:
    return float(1.0 / (1.0 + np.exp(-x)))


def logit(p: float) -> float:
    p = clamp01(p, 1e-6, 1 - 1e-6)
    return float(np.log(p / (1 - p)))


class Progress:
    def __init__(self, total: int, every: int = 500, label: str = "progress"):
        self.total = int(total)
        self.every = int(max(1, every))
        self.label = label
        self.t0 = time.time()
        self.last_t = self.t0

    def tick(self, i: int, extra: str = ""):
        # i is 0-based index of completed items; show at every N or at end
        if (i + 1) % self.every != 0 and (i + 1) != self.total:
            return

        now = time.time()
        elapsed = now - self.t0
        done = i + 1
        rate = done / elapsed if elapsed > 0 else float("inf")
        remaining = self.total - done
        eta = remaining / rate if rate > 0 else float("inf")

        msg = (
            f"[{self.label}] {done:,}/{self.total:,} "
            f"({done/self.total:6.2%}) | "
            f"elapsed {elapsed:7.1f}s | "
            f"rate {rate:7.2f}/s | "
            f"ETA {eta:7.1f}s"
        )
        if extra:
            msg += f" | {extra}"

        print(msg, flush=True)
        self.last_t = now

# -------------------------
# 1) Markov model builders
# -------------------------
def build_matrix(transitions: Dict[int, Dict[int, float]], states: Dict[int, str]) -> np.ndarray:
    state_ids_local = list(states.keys())
    n = len(state_ids_local)
    col = {sid: i for i, sid in enumerate(state_ids_local)}
    M = np.zeros((n, n), dtype=float)

    for s_from, row in transitions.items():
        for s_to, p in row.items():
            M[col[s_from], col[s_to]] = float(p)

    row_sums = M.sum(axis=1)
    if not np.allclose(row_sums, 1.0, atol=1e-8):
        bad = [(state_ids_local[i], float(row_sums[i])) for i in range(n) if not np.isclose(row_sums[i], 1.0)]
        raise ValueError(f"Rows not summing to 1. Offenders (state_id, sum): {bad}")
    return M


def maturity_lambda(day_idx: int, midpoint: int = 120, steepness: float = 0.06) -> float:
    # λ(t) in (0,1): slow->fast->slow maturity curve
    return sigmoid(steepness * (day_idx - midpoint))


def experiment_ramp(day_idx: int, start_day: int, ramp_steepness: float = 0.15) -> float:
    # g(t): 0->1 gradual rollout
    return sigmoid(ramp_steepness * (day_idx - start_day))


def interpolate_P(P_startup: np.ndarray, P_mature: np.ndarray, lam: float) -> np.ndarray:
    lam = float(np.clip(lam, 0.0, 1.0))
    P = (1.0 - lam) * P_startup + lam * P_mature
    return P / P.sum(axis=1, keepdims=True)


def apply_mass_shift(row: np.ndarray, add_to: int, take_from: List[int], delta: float) -> np.ndarray:
    """
    Move `delta` probability mass from take_from columns to add_to column.
    Preserves row-stochasticity.
    """
    row = row.copy()
    delta = float(max(0.0, delta))
    available = float(row[take_from].sum())
    if available <= 0.0 or delta <= 0.0:
        return row

    d = min(delta, available)
    row[take_from] -= d * (row[take_from] / available)
    row[add_to] += d
    return row / row.sum()


def P_base_for_day(day_idx: int, P_startup: np.ndarray, P_mature: np.ndarray) -> np.ndarray:
    lam = maturity_lambda(day_idx)
    return interpolate_P(P_startup, P_mature, lam)


def P_control_for_day(day_idx: int, P_startup: np.ndarray, P_mature: np.ndarray) -> np.ndarray:
    return P_base_for_day(day_idx, P_startup, P_mature)


def P_treatment_for_day(
    day_idx: int,
    exp_start_day: int,
    P_startup: np.ndarray,
    P_mature: np.ndarray,
    delta0_new: float = 0.12,
    delta0_wau: float = 0.08
) -> np.ndarray:
    """
    Option 3: treatment ramps in (g(t)) and fades out as product matures (1-λ(t)).
    Modifies only selected rows (New Users=2, At Risk WAUs=5):
      shift mass from cols {At Risk MAU=6, Dormant=7} -> Current=4
    """
    P = P_base_for_day(day_idx, P_startup, P_mature).copy()

    lam = maturity_lambda(day_idx)
    g = experiment_ramp(day_idx, start_day=exp_start_day)
    scale = g * (0.7 + 0.3 * (1 - lam))

    row_new = IDX[2]
    row_wau = IDX[5]
    col_current = IDX[4]
    take_from = [IDX[6], IDX[7]]

    P[row_new, :] = apply_mass_shift(P[row_new, :], add_to=col_current, take_from=take_from, delta=delta0_new * scale)
    P[row_wau, :] = apply_mass_shift(P[row_wau, :], add_to=col_current, take_from=take_from, delta=delta0_wau * scale)
    return P


# -------------------------
# 2) Activity + streak model
# -------------------------
def alpha_streak(day_idx: int, variant: str, exp_start_day: int, alpha_base: float = 0.25, kappa: float = 0.60) -> float:
    g = experiment_ramp(day_idx, start_day=exp_start_day)
    if variant == "treatment":
        return alpha_base * (1.0 + kappa * g)
    return alpha_base


def p_active_today(
    state_id: int,
    streak_len: int,
    variant: str,
    day_idx: int,
    exp_start_day: int,
    p_active_base: Dict[int, float],
    user_effect: float = 0.0,
    dow_effect: float = 0.0
) -> float:
    """
    logit(p_t) = logit(p0(state)) + alpha(t)*log(1+streak) + user_effect + dow_effect
    """
    p0 = float(p_active_base[state_id])
    a = alpha_streak(day_idx, variant, exp_start_day)
    z = logit(p0) + a * float(np.log1p(streak_len)) + float(user_effect) + float(dow_effect)
    return float(np.clip(sigmoid(z), 0.0, 0.98))


# -------------------------
# 3) Schema generation config
# -------------------------
@dataclass
class SimConfig:
    n_users: int = 50000
    n_days: int = 365
    start_date: date = date(2025, 1, 1)  # synthetic calendar anchor
    exp_start_day: int = 90
    seed: int = 42
    out_dir: str = "data"

    # signups
    signup_midpoint: int = 120
    signup_steepness: float = 0.05
    min_daily_signups: int = 20
    max_daily_signups: int = 250

    # sessionization
    avg_sessions_per_active_day: float = 1.2
    max_sessions_per_active_day: int = 3

    # lesson funnel
    p_do_lesson_given_open: float = 0.78
    p_complete_given_start: float = 0.86
    mean_questions_per_lesson: float = 10.0

    # paywall / purchase (optional realism)
    p_paywall_on_active_day: float = 0.04
    p_purchase_given_paywall: float = 0.18


# -------------------------
# 4) Lessons dimension table
# -------------------------
def generate_lessons(rng: np.random.Generator, n_units: int = 8, skills_per_unit: int = 4, lessons_per_skill: int = 8) -> pd.DataFrame:
    rows = []
    topics = ["Travel", "Food", "Work", "Daily Life", "Culture", "School", "Shopping", "Health"]
    skills = ["Basics", "Past Tense", "Future", "Questions", "Pronouns", "Greetings", "Numbers", "Directions", "Food Vocab", "Workplace"]

    lesson_idx = 0
    for u in range(1, n_units + 1):
        unit_id = f"U{u:02d}"
        for s in range(skills_per_unit):
            skill = skills[(u + s) % len(skills)]
            for l in range(1, lessons_per_skill + 1):
                lesson_idx += 1
                difficulty = int(np.clip(1 + (u // 2) + (l // 4), 1, 5))
                expected_duration_sec = int(np.clip(rng.normal(180 + 20 * difficulty, 40), 90, 420))
                expected_xp = int(np.clip(10 + 3 * difficulty + rng.integers(-2, 3), 8, 25))
                topic = topics[(lesson_idx + u) % len(topics)]
                lesson_id = f"L_{unit_id}_S{s+1:02d}_{l:02d}"
                rows.append({
                    "lesson_id": lesson_id,
                    "unit_id": unit_id,
                    "skill": skill,
                    "difficulty": difficulty,
                    "topic": topic,
                    "expected_duration_sec": expected_duration_sec,
                    "expected_xp": expected_xp,
                })

    return pd.DataFrame(rows)


# -------------------------
# 5) Users table
# -------------------------
def daily_signup_curve(day_idx: int, cfg: SimConfig) -> float:
    # a smooth curve just to vary daily signups (peaks mid-year-ish)
    x = (day_idx - cfg.signup_midpoint) * cfg.signup_steepness
    return sigmoid(x)


def allocate_signups(cfg: SimConfig, rng: np.random.Generator) -> List[int]:
    """
    Return list of length n_days: signups per day, summing to n_users.
    """
    weights = np.array([daily_signup_curve(t, cfg) for t in range(cfg.n_days)], dtype=float)
    weights = weights / weights.sum()

    raw = rng.multinomial(cfg.n_users, weights)

    # enforce min/max bounds softly by redistribution
    signups = raw.astype(int).tolist()

    # cap above max
    overflow = 0
    for t in range(cfg.n_days):
        if signups[t] > cfg.max_daily_signups:
            overflow += signups[t] - cfg.max_daily_signups
            signups[t] = cfg.max_daily_signups

    # raise below min
    deficit = 0
    for t in range(cfg.n_days):
        if signups[t] < cfg.min_daily_signups:
            deficit += cfg.min_daily_signups - signups[t]
            signups[t] = cfg.min_daily_signups

    # reconcile total to cfg.n_users
    total = sum(signups)
    target = cfg.n_users
    diff = total - target  # positive means too many

    # absorb diff by adjusting days with room
    if diff != 0:
        order = list(range(cfg.n_days))
        rng.shuffle(order)
        for t in order:
            if diff == 0:
                break
            if diff > 0:
                # reduce if above min
                room = signups[t] - cfg.min_daily_signups
                if room > 0:
                    dec = min(room, diff)
                    signups[t] -= dec
                    diff -= dec
            else:
                # increase if below max
                room = cfg.max_daily_signups - signups[t]
                if room > 0:
                    inc = min(room, -diff)
                    signups[t] += inc
                    diff += inc

    # final sanity
    assert sum(signups) == cfg.n_users, f"Signups sum mismatch: {sum(signups)} vs {cfg.n_users}"
    return signups


def sample_user_attributes(rng: np.random.Generator) -> Dict[str, object]:
    signup_channel = rng.choice(["organic", "paid", "referral"], p=[0.62, 0.28, 0.10])
    country = rng.choice(COUNTRIES, p=[0.22, 0.38, 0.40])
    language_target = rng.choice(LANG_TARGETS, p=[0.5, 0.3, 0.2])
    device_os = rng.choice(DEVICE_OS, p=[0.46, 0.46, 0.08])
    app_version = rng.choice(APP_VERSIONS, p=[0.45, 0.35, 0.20])
    timezone = "Australia/Perth"
    signup_platform = rng.choice(SIGNUP_PLATFORM, p=[0.78, 0.22])

    # premium at signup probability varies by channel/country (simple heuristic)
    base_prem = 0.06
    if signup_channel == "paid":
        base_prem += 0.04
    if country == "US":
        base_prem += 0.03
    if device_os == "Web":
        base_prem -= 0.01
    is_premium_at_signup = (rng.random() < np.clip(base_prem, 0.01, 0.20))

    # campaign id only if paid
    campaign_id = None
    if signup_channel == "paid":
        campaign_id = rng.choice(["g_ads_2025w01", "meta_2025w06", "tiktok_2025w10"])

    return {
        "signup_channel": signup_channel,
        "campaign_id": campaign_id,
        "country": country,
        "language_target": language_target,
        "device_os": device_os,
        "app_version": app_version,
        "timezone": timezone,
        "signup_platform": signup_platform,
        "is_premium_at_signup": bool(is_premium_at_signup),
    }


def generate_users(cfg: SimConfig, rng: np.random.Generator) -> pd.DataFrame:
    signups_per_day = allocate_signups(cfg, rng)

    rows = []
    user_id = 100000  # deterministic start
    for day_idx, n_signup in enumerate(signups_per_day):
        d = cfg.start_date + timedelta(days=day_idx)
        for _ in range(n_signup):
            user_id += 1
            attrs = sample_user_attributes(rng)
            # signup_time: local time within day
            hh = int(rng.integers(6, 23))
            mm = int(rng.integers(0, 60))
            ss = int(rng.integers(0, 60))
            signup_time = datetime(d.year, d.month, d.day, hh, mm, ss)

            rows.append({
                "user_id": user_id,
                "signup_date": d.isoformat(),
                "signup_time": signup_time.isoformat(sep=" "),
                **attrs,
            })

    users = pd.DataFrame(rows)
    users["campaign_id"] = users["campaign_id"].fillna("")
    return users


# -------------------------
# 6) Sessions + Events generation
# -------------------------
def make_session_id(counter: int) -> str:
    return f"sess_{counter:08d}"


def make_event_id(counter: int) -> str:
    return f"evt_{counter:09d}"


def dow_effect(day_idx: int) -> float:
    """
    Small weekly seasonality in logit space.
    (Mon..Sun) => encourage weekend slightly.
    """
    # day 0 corresponds to cfg.start_date; we'll treat it as "weekday-like"
    # We'll implement a mild sine wave with weekly period.
    return 0.10 * np.sin(2 * np.pi * (day_idx % 7) / 7.0)


def choose_variant_for_user(rng: np.random.Generator) -> str:
    # fixed 50/50 assignment at signup for Phase 1 simplicity
    return str(rng.choice(["control", "treatment"]))


def choose_experiment_id() -> str:
    return "exp_streak_nudge"


def sample_user_random_effect(rng: np.random.Generator) -> float:
    # logit-scale heterogeneity
    return float(rng.normal(0.0, 0.15))


def pick_lesson(lessons_df: pd.DataFrame, rng: np.random.Generator, user_language_target: str) -> str:
    # In Phase 1, lesson_id independent of language_target (kept simple).
    # You can later build separate lesson catalogs per language.
    idx = int(rng.integers(0, len(lessons_df)))
    return str(lessons_df.iloc[idx]["lesson_id"])


def generate_day_events_for_user(
    *,
    rng: np.random.Generator,
    lessons_df: pd.DataFrame,
    user_row: pd.Series,
    user_id: int,
    variant: str,
    experiment_id: str,
    day_idx_global: int,
    day_date: date,
    state_id: int,
    streak_len_start: int,
    is_premium_current: bool,
    cfg: SimConfig,
    session_counter_start: int,
    event_counter_start: int, 
    min_start_dt: Optional[datetime] = None
) -> Tuple[
    List[dict],  # sessions rows
    List[dict],  # events rows
    int,         # session_counter_end
    int,         # event_counter_end
    int,         # streak_len_end
    bool         # is_premium_end
]:
    """
    Create events (and sessions) for one user on one active day.
    Ensures:
      - event_time within sessions
      - session_end >= session_start
      - lesson_completed has lesson_started earlier in same session (probabilistic but enforced here)
      - streak_length field updated on relevant events
    """
    sessions_rows: List[dict] = []
    events_rows: List[dict] = []

    # number of sessions today
    lam = cfg.avg_sessions_per_active_day
    n_sess = int(np.clip(rng.poisson(lam=lam), 1, cfg.max_sessions_per_active_day))

    # baseline day start time; spread sessions
    base_hour = int(rng.integers(6, 20))
    base_min = int(rng.integers(0, 60))
    day_start_dt = datetime(day_date.year, day_date.month, day_date.day, base_hour, base_min, int(rng.integers(0, 60)))
    if min_start_dt is not None and day_start_dt < min_start_dt:
        day_start_dt = min_start_dt
    # define a hard day boundary
    day_end_dt = datetime(day_date.year, day_date.month, day_date.day, 23, 59, 50)

    streak_len = streak_len_start
    session_counter = session_counter_start
    event_counter = event_counter_start

    # optional push mechanics before first open
    if rng.random() < 0.15:
        event_counter += 1

        evt_time = day_start_dt - timedelta(minutes=int(rng.integers(5, 120)))

        # clamp: on signup day, push cannot precede signup_time; otherwise clamp to day start
        if min_start_dt is not None:
            evt_time = max(evt_time, min_start_dt)
        else:
            evt_time = max(evt_time, datetime(day_date.year, day_date.month, day_date.day, 0, 0, 5))

        evt_time = min(evt_time, day_end_dt)

        events_rows.append({
            "event_id": make_event_id(event_counter),
            "user_id": user_id,
            "session_id": "",
            "event_time": evt_time.isoformat(sep=" "),
            "event_date": day_date.isoformat(),
            "event_name": "push_received",
            "screen": "home",
            "language_target": user_row["language_target"],
            "lesson_id": "",
            "xp_delta": 0,
            "hearts_delta": 0,
            "streak_length": streak_len,
            "is_premium": is_premium_current,
            "experiment_id": experiment_id,
            "variant": variant,
        })

        # push opened sometimes
        if rng.random() < 0.35:
            event_counter += 1
            evt_time2 = evt_time + timedelta(minutes=int(rng.integers(1, 30)))
            evt_time2 = min(evt_time2, day_end_dt)
            if min_start_dt is not None:
                evt_time2 = max(evt_time2, min_start_dt)

            events_rows.append({
                "event_id": make_event_id(event_counter),
                "user_id": user_id,
                "session_id": "",
                "event_time": evt_time2.isoformat(sep=" "),
                "event_date": day_date.isoformat(),
                "event_name": "push_opened",
                "screen": "home",
                "language_target": user_row["language_target"],
                "lesson_id": "",
                "xp_delta": 0,
                "hearts_delta": 0,
                "streak_length": streak_len,
                "is_premium": is_premium_current,
                "experiment_id": experiment_id,
                "variant": variant,
            })

    current_time = day_start_dt
    for s in range(n_sess):
        session_counter += 1
        session_id = make_session_id(session_counter)

        # session duration
        dur_sec = int(np.clip(rng.normal(9 * 60, 4 * 60), 60, 45 * 60))
        session_start = current_time + timedelta(minutes=int(rng.integers(0, 60)))
        session_end = session_start + timedelta(seconds=dur_sec)

        # keep session inside the same calendar day
        MIN_SESSION_SEC = 60  # choose 60 or 90

        # keep session inside day
        if session_start > day_end_dt:
            session_start = day_end_dt - timedelta(seconds=MIN_SESSION_SEC)

        if session_end > day_end_dt:
            session_end = day_end_dt

        # enforce minimum duration AFTER capping
        if (session_end - session_start).total_seconds() < MIN_SESSION_SEC:
            session_start = day_end_dt - timedelta(seconds=MIN_SESSION_SEC)
            session_end = day_end_dt

        dur_sec = int((session_end - session_start).total_seconds())

        sessions_rows.append({
            "session_id": session_id,
            "user_id": user_id,
            "session_start": session_start.isoformat(sep=" "),
            "session_end": session_end.isoformat(sep=" "),
            "session_duration_sec": dur_sec,
            "device_os": user_row["device_os"],
            "app_version": user_row["app_version"],
        })

        # app_open at session_start
        event_counter += 1
        events_rows.append({
            "event_id": make_event_id(event_counter),
            "user_id": user_id,
            "session_id": session_id,
            "event_time": session_start.isoformat(sep=" "),
            "event_date": day_date.isoformat(),
            "event_name": "app_open",
            "screen": "home",
            "language_target": user_row["language_target"],
            "lesson_id": "",
            "xp_delta": 0,
            "hearts_delta": 0,
            "streak_length": streak_len,
            "is_premium": is_premium_current,
            "experiment_id": experiment_id,
            "variant": variant,
        })

        # lesson flow in this session?
        if rng.random() < cfg.p_do_lesson_given_open:
            lesson_id = pick_lesson(lessons_df, rng, user_row["language_target"])
            # lesson_started
            event_counter += 1
            t_lesson_start = session_start + timedelta(seconds=int(rng.integers(5, 90)))
            t_lesson_start = min(t_lesson_start, session_end - timedelta(seconds=10))
            t_lesson_start = max(t_lesson_start, session_start + timedelta(seconds=1))
            events_rows.append({
                "event_id": make_event_id(event_counter),
                "user_id": user_id,
                "session_id": session_id,
                "event_time": t_lesson_start.isoformat(sep=" "),
                "event_date": day_date.isoformat(),
                "event_name": "lesson_started",
                "screen": "lesson",
                "language_target": user_row["language_target"],
                "lesson_id": lesson_id,
                "xp_delta": 0,
                "hearts_delta": 0,
                "streak_length": streak_len,
                "is_premium": is_premium_current,
                "experiment_id": experiment_id,
                "variant": variant,
            })

            # questions answered
            n_q = int(np.clip(rng.poisson(cfg.mean_questions_per_lesson), 3, 25))
            hearts_delta_total = 0
            t_cursor = t_lesson_start
            for _ in range(n_q):
                event_counter += 1
                t_cursor = t_cursor + timedelta(seconds=int(rng.integers(3, 15)))
                t_cursor = min(t_cursor, session_end - timedelta(seconds=6))
                # mistakes occasionally
                hearts_delta = 0
                if rng.random() < 0.08:
                    hearts_delta = -1
                    hearts_delta_total += hearts_delta
                events_rows.append({
                    "event_id": make_event_id(event_counter),
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_time": t_cursor.isoformat(sep=" "),
                    "event_date": day_date.isoformat(),
                    "event_name": "question_answered",
                    "screen": "lesson",
                    "language_target": user_row["language_target"],
                    "lesson_id": lesson_id,
                    "xp_delta": 0,
                    "hearts_delta": hearts_delta,
                    "streak_length": streak_len,
                    "is_premium": is_premium_current,
                    "experiment_id": experiment_id,
                    "variant": variant,
                })

            # lesson_completed (most times)
            if rng.random() < cfg.p_complete_given_start:
                lesson_row = lessons_df.loc[lessons_df["lesson_id"] == lesson_id].iloc[0]
                xp = int(lesson_row["expected_xp"])
                event_counter += 1
                t_complete = min(session_end - timedelta(seconds=5), t_cursor + timedelta(seconds=int(rng.integers(15, 80))))
                events_rows.append({
                    "event_id": make_event_id(event_counter),
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_time": t_complete.isoformat(sep=" "),
                    "event_date": day_date.isoformat(),
                    "event_name": "lesson_completed",
                    "screen": "lesson_result",
                    "language_target": user_row["language_target"],
                    "lesson_id": lesson_id,
                    "xp_delta": max(0, xp),
                    "hearts_delta": 0,
                    "streak_length": streak_len,
                    "is_premium": is_premium_current,
                    "experiment_id": experiment_id,
                    "variant": variant,
                })

        # paywall/purchase events (optional realism)
        if (not is_premium_current) and (rng.random() < cfg.p_paywall_on_active_day):
            event_counter += 1

            # base paywall time, then clamp inside session
            t_paywall = session_end - timedelta(seconds=int(rng.integers(10, 120)))
            t_paywall = max(t_paywall, session_start + timedelta(seconds=1))
            t_paywall = min(t_paywall, session_end - timedelta(seconds=1))

            events_rows.append({
                "event_id": make_event_id(event_counter),
                "user_id": user_id,
                "session_id": session_id,
                "event_time": t_paywall.isoformat(sep=" "),
                "event_date": day_date.isoformat(),
                "event_name": "paywall_shown",
                "screen": "shop",
                "language_target": user_row["language_target"],
                "lesson_id": "",
                "xp_delta": 0,
                "hearts_delta": 0,
                "streak_length": streak_len,
                "is_premium": is_premium_current,
                "experiment_id": experiment_id,
                "variant": variant,
            })

            if rng.random() < cfg.p_purchase_given_paywall:
                is_premium_current = True
                event_counter += 1

                t_purchase = t_paywall + timedelta(seconds=int(rng.integers(5, 60)))
                t_purchase = max(t_purchase, session_start + timedelta(seconds=2))
                t_purchase = min(t_purchase, session_end - timedelta(seconds=2))

                events_rows.append({
                    "event_id": make_event_id(event_counter),
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_time": t_purchase.isoformat(sep=" "),
                    "event_date": day_date.isoformat(),
                    "event_name": "purchase",
                    "screen": "shop",
                    "language_target": user_row["language_target"],
                    "lesson_id": "",
                    "xp_delta": 0,
                    "hearts_delta": 0,
                    "streak_length": streak_len,
                    "is_premium": is_premium_current,
                    "experiment_id": experiment_id,
                    "variant": variant,
                })


        # app_background near end
        event_counter += 1
        events_rows.append({
            "event_id": make_event_id(event_counter),
            "user_id": user_id,
            "session_id": session_id,
            "event_time": session_end.isoformat(sep=" "),
            "event_date": day_date.isoformat(),
            "event_name": "app_background",
            "screen": rng.choice(SCREENS),
            "language_target": user_row["language_target"],
            "lesson_id": "",
            "xp_delta": 0,
            "hearts_delta": 0,
            "streak_length": streak_len,
            "is_premium": is_premium_current,
            "experiment_id": experiment_id,
            "variant": variant,
        })

        current_time = session_end

    # streak increment event (end of day)
    streak_len = streak_len + 1
    event_counter += 1
    streak_time = min(current_time + timedelta(minutes=5), day_end_dt)
    events_rows.append({
        "event_id": make_event_id(event_counter),
        "user_id": user_id,
        "session_id": "",
        "event_time": streak_time.isoformat(sep=" "),
        "event_date": day_date.isoformat(),
        "event_name": "streak_incremented",
        "screen": "home",
        "language_target": user_row["language_target"],
        "lesson_id": "",
        "xp_delta": 0,
        "hearts_delta": 0,
        "streak_length": streak_len,
        "is_premium": is_premium_current,
        "experiment_id": experiment_id,
        "variant": variant,
    })

    return sessions_rows, events_rows, session_counter, event_counter, streak_len, is_premium_current


def simulate(cfg: SimConfig) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(cfg.seed)
    print("=== simulate(): starting ===", flush=True)
    print("Stage 1/6: building transition matrices...", flush=True)

    # ----- transitions (you already defined these; included here for completeness) -----
    startup_transitions = {
        1: {4: 0.55, 5: 0.20, 6: 0.15, 7: 0.10},
        2: {4: 0.25, 5: 0.35, 6: 0.25, 7: 0.15},
        3: {4: 0.35, 5: 0.25, 6: 0.25, 7: 0.15},
        4: {4: 0.70, 5: 0.15, 6: 0.10, 7: 0.05},
        5: {4: 0.20, 5: 0.35, 6: 0.25, 7: 0.20},
        6: {4: 0.10, 5: 0.15, 6: 0.35, 7: 0.40},
        7: {1: 0.03, 3: 0.02, 7: 0.95},
    }
    mature_transitions = {
        1: {4: 0.70, 5: 0.15, 6: 0.10, 7: 0.05},
        2: {4: 0.40, 5: 0.30, 6: 0.20, 7: 0.10},
        3: {4: 0.55, 5: 0.20, 6: 0.15, 7: 0.10},
        4: {4: 0.88, 5: 0.07, 6: 0.03, 7: 0.02},
        5: {4: 0.45, 5: 0.35, 6: 0.12, 7: 0.08},
        6: {4: 0.25, 5: 0.20, 6: 0.35, 7: 0.20},
        7: {1: 0.06, 3: 0.04, 7: 0.90},
    }
    P_startup = build_matrix(startup_transitions, STATES)
    P_mature = build_matrix(mature_transitions, STATES)

    print("Stage 2/6: generating lessons...", flush=True)

    # ----- base activity by state -----
    p_active_base = {
        1: 0.55,  # Reactivated
        2: 0.45,  # New
        3: 0.35,  # Resurrected
        4: 0.75,  # Current
        5: 0.30,  # At Risk WAU
        6: 0.12,  # At Risk MAU
        7: 0.02,  # Dormant
    }

    # ----- tables -----
    lessons = generate_lessons(rng)
    print("Stage 3/6: generating users (signup allocation)...", flush=True)

    users = generate_users(cfg, rng)
    print("Stage 4/6: simulating user-day activity + sessions/events...", flush=True)

    # add experiment assignment (variant) — balanced within each signup_date
    users["variant"] = None

    for d, idx in users.groupby("signup_date").groups.items():
        idx = list(idx)
        rng.shuffle(idx)
        n = len(idx)
        half = n // 2

        users.loc[idx[:half], "variant"] = "control"
        users.loc[idx[half:], "variant"] = "treatment"

    assert users["variant"].isna().sum() == 0


    # user random effects
    users["user_effect"] = [sample_user_random_effect(rng) for _ in range(len(users))]

    # Simulate per user from signup day to end horizon
    sessions_rows: List[dict] = []
    events_rows: List[dict] = []

    session_counter = 0
    event_counter = 0

    # Pre-create mapping signup_date -> day index
    start_dt = cfg.start_date
    signup_day_idx = pd.to_datetime(users["signup_date"]).dt.date.apply(lambda d: (d - start_dt).days).astype(int)
    users["signup_day_idx"] = signup_day_idx

    experiment_id = choose_experiment_id()

    # We’ll store lifecycle state path summary in-memory (optional)
    # For Phase 1 schema, we do not output a "states" table—only sessions/events reflect behavior.

    prog = Progress(total=len(users), every=max(100, len(users)//200), label="users")  # ~200 updates total max
    t_loop0 = time.time()
    events_last = 0
    sessions_last = 0

    last_heartbeat = time.time()
    for i in range(len(users)):
        u = users.iloc[i]
        user_id = int(u["user_id"])
        var = str(u["variant"])
        u_eff = float(u["user_effect"])
        prem = bool(u["is_premium_at_signup"])

        # initialize state and streak at signup
        current_state = 2  # New
        streak_len = 0

        # signup + onboarding events on signup day (even if not "active" by model)
        s_day_idx = int(u["signup_day_idx"])
        s_date = cfg.start_date + timedelta(days=s_day_idx)

        # signup event
        event_counter += 1
        signup_time = datetime.fromisoformat(str(u["signup_time"]))
        events_rows.append({
            "event_id": make_event_id(event_counter),
            "user_id": user_id,
            "session_id": "",
            "event_time": signup_time.isoformat(sep=" "),
            "event_date": s_date.isoformat(),
            "event_name": "signup",
            "screen": "onboarding",
            "language_target": u["language_target"],
            "lesson_id": "",
            "xp_delta": 0,
            "hearts_delta": 0,
            "streak_length": 0,
            "is_premium": prem,
            "experiment_id": experiment_id,
            "variant": var,
        })

        # onboarding completed shortly after signup
        event_counter += 1
        events_rows.append({
            "event_id": make_event_id(event_counter),
            "user_id": user_id,
            "session_id": "",
            "event_time": (signup_time + timedelta(minutes=int(rng.integers(2, 18)))).isoformat(sep=" "),
            "event_date": s_date.isoformat(),
            "event_name": "onboarding_completed",
            "screen": "onboarding",
            "language_target": u["language_target"],
            "lesson_id": "",
            "xp_delta": 0,
            "hearts_delta": 0,
            "streak_length": 0,
            "is_premium": prem,
            "experiment_id": experiment_id,
            "variant": var,
        })

        # simulate days from signup to end
        for day_idx in range(s_day_idx, cfg.n_days):
            d = cfg.start_date + timedelta(days=day_idx)

            # pick matrix for today (affects tomorrow's state)
            if var == "control":
                P_today = P_control_for_day(day_idx, P_startup, P_mature)
            else:
                P_today = P_treatment_for_day(day_idx, cfg.exp_start_day, P_startup, P_mature)

            # activity draw
            p = p_active_today(
                state_id=current_state,
                streak_len=streak_len,
                variant=var,
                day_idx=day_idx,
                exp_start_day=cfg.exp_start_day,
                p_active_base=p_active_base,
                user_effect=u_eff,
                dow_effect=dow_effect(day_idx),
            )
            active = (rng.random() < p)

            min_start_dt = None
            if day_idx == s_day_idx:
                min_start_dt = signup_time + timedelta(minutes=2)

            if active:
                s_rows, e_rows, session_counter, event_counter, streak_len, prem = generate_day_events_for_user(
                    rng=rng,
                    lessons_df=lessons,
                    user_row=u,
                    user_id=user_id,
                    variant=var,
                    experiment_id=experiment_id,
                    day_idx_global=day_idx,
                    day_date=d,
                    state_id=current_state,
                    streak_len_start=streak_len,
                    is_premium_current=prem,
                    cfg=cfg,
                    session_counter_start=session_counter,
                    event_counter_start=event_counter, 
                    min_start_dt=min_start_dt
                )
                sessions_rows.extend(s_rows)
                events_rows.extend(e_rows)
            else:
                # if inactive, streak breaks (optional event)
                if streak_len > 0:
                    streak_len = 0
                    event_counter += 1
                    events_rows.append({
                        "event_id": make_event_id(event_counter),
                        "user_id": user_id,
                        "session_id": "",
                        "event_time": datetime(d.year, d.month, d.day, 23, 30, 0).isoformat(sep=" "),
                        "event_date": d.isoformat(),
                        "event_name": "streak_broken",
                        "screen": "home",
                        "language_target": u["language_target"],
                        "lesson_id": "",
                        "xp_delta": 0,
                        "hearts_delta": 0,
                        "streak_length": 0,
                        "is_premium": prem,
                        "experiment_id": experiment_id,
                        "variant": var,
                    })

            # transition to next day's state
            current_state = int(rng.choice(STATE_IDS, p=P_today[IDX[current_state], :]))

        # progress heartbeat per user
        if (i + 1) % prog.every == 0 or (i + 1) == len(users):
            # how many rows added since last print
            ev_now = len(events_rows)
            se_now = len(sessions_rows)
            delta_ev = ev_now - events_last
            delta_se = se_now - sessions_last
            events_last = ev_now
            sessions_last = se_now

            extra = f"rows(+{delta_se:,} sessions, +{delta_ev:,} events)"
            prog.tick(i, extra=extra)

        now = time.time()
        if now - last_heartbeat > 10:  # every 10 seconds
            print(f"(heartbeat) i={i+1:,}/{len(users):,} events={len(events_rows):,} sessions={len(sessions_rows):,}", flush=True)
            last_heartbeat = now


    print("Stage 5/6: assembling dataframes...", flush=True)
    sessions = pd.DataFrame(sessions_rows)
    events = pd.DataFrame(events_rows)

    print("Stage 6/6: sorting & type casting...", flush=True)

    # ensure ordering + types
    if len(sessions) > 0:
        sessions["session_start"] = pd.to_datetime(sessions["session_start"])
        sessions["session_end"] = pd.to_datetime(sessions["session_end"])
        sessions["session_duration_sec"] = sessions["session_duration_sec"].astype(int)
        sessions = sessions.sort_values(["user_id", "session_start"]).reset_index(drop=True)

    if len(events) > 0:
        events["event_time"] = pd.to_datetime(events["event_time"])
        events["event_date"] = events["event_time"].dt.date.astype(str)
        events = events.sort_values(["user_id", "event_time"]).reset_index(drop=True)

    # users cleanup (schema columns only)
    users_out = users[[
        "user_id", "signup_date", "signup_time",
        "signup_channel", "campaign_id", "country", "language_target",
        "device_os", "app_version", "timezone", "signup_platform",
        "is_premium_at_signup",
        # keep these as "extra" helpful fields for A/B + modeling
        "variant",
    ]].copy()

    # sessions schema already aligned
    sessions_out = sessions[[
        "session_id", "user_id", "session_start", "session_end",
        "session_duration_sec", "device_os", "app_version"
    ]].copy() if len(sessions) else pd.DataFrame(columns=[
        "session_id", "user_id", "session_start", "session_end",
        "session_duration_sec", "device_os", "app_version"
    ])

    # events schema aligned
    events_out = events[[
        "event_id", "user_id", "session_id", "event_time", "event_date",
        "event_name", "screen", "language_target", "lesson_id",
        "xp_delta", "hearts_delta", "streak_length", "is_premium",
        "experiment_id", "variant"
    ]].copy() if len(events) else pd.DataFrame(columns=[
        "event_id", "user_id", "session_id", "event_time", "event_date",
        "event_name", "screen", "language_target", "lesson_id",
        "xp_delta", "hearts_delta", "streak_length", "is_premium",
        "experiment_id", "variant"
    ])

    # lessons schema aligned
    lessons_out = lessons[[
        "lesson_id", "unit_id", "skill", "difficulty", "topic",
        "expected_duration_sec", "expected_xp"
    ]].copy()

    return users_out, sessions_out, events_out, lessons_out


# -------------------------
# 7) Sanity checks (fast)
# -------------------------
def run_sanity_checks(users: pd.DataFrame, sessions: pd.DataFrame, events: pd.DataFrame, lessons: pd.DataFrame) -> None:
    # key constraints
    assert users["user_id"].is_unique
    assert lessons["lesson_id"].is_unique
    if len(sessions):
        assert sessions["session_id"].is_unique
        assert sessions["user_id"].isin(users["user_id"]).all()
        assert (sessions["session_end"] >= sessions["session_start"]).all()
        assert (sessions["session_duration_sec"] >= 0).all()

    if len(events):
        assert events["event_id"].is_unique
        assert events["user_id"].isin(users["user_id"]).all()

        # allowed event names
        bad_names = set(events["event_name"].unique()) - EVENT_TAXONOMY
        assert not bad_names, f"Unknown event names: {bad_names}"

        # lesson_id existence when needed
        lesson_flow = events["event_name"].isin(["lesson_started", "question_answered", "lesson_completed"])
        if lesson_flow.any():
            # allow empty lesson_id only if not lesson_flow
            assert (events.loc[lesson_flow, "lesson_id"] != "").all()
            assert events.loc[lesson_flow, "lesson_id"].isin(lessons["lesson_id"]).all()

        # xp nonnegative on completed
        comp = events["event_name"].eq("lesson_completed")
        if comp.any():
            assert (events.loc[comp, "xp_delta"] >= 0).all()

        assert (events["streak_length"] >= 0).all()

    # quick health print
    print("\n--- Sanity summary ---")
    print(f"users   : {len(users):,}")
    print(f"sessions: {len(sessions):,}")
    print(f"events  : {len(events):,}")
    print(f"lessons : {len(lessons):,}")

    if len(events):
        active_events = events["event_name"].isin(["app_open", "lesson_started", "lesson_completed"])
        dau = events.loc[active_events].groupby("event_date")["user_id"].nunique()
        print(f"DAU range: {int(dau.min()):,} .. {int(dau.max()):,}  (days={len(dau)})")

        # rough A/B activity lift: compare avg active-days proxy by app_open counts/user
        opens = events[events["event_name"] == "app_open"].groupby(["user_id"])["event_id"].count()
        uvar = users.set_index("user_id")["variant"]
        df = pd.DataFrame({"opens": opens}).join(uvar, how="left").fillna({"opens": 0})
        by = df.groupby("variant")["opens"].mean()
        print("mean app_opens per user by variant:")
        print(by)


# -------------------------
# 8) CLI
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n_users", type=int, default=10000)
    ap.add_argument("--n_days", type=int, default=365)
    ap.add_argument("--seed", type=int, default=1239)
    ap.add_argument("--out_dir", type=str, default="data")
    ap.add_argument("--start_date", type=str, default="2025-01-01")
    ap.add_argument("--exp_start_day", type=int, default=90)
    args = ap.parse_args()

    cfg = SimConfig(
        n_users=args.n_users,
        n_days=args.n_days,
        seed=args.seed,
        out_dir=args.out_dir,
        start_date=datetime.fromisoformat(args.start_date).date(),
        exp_start_day=args.exp_start_day,
    )

    ensure_dir(cfg.out_dir)

    users, sessions, events, lessons = simulate(cfg)
    run_sanity_checks(users, sessions, events, lessons)

    users.to_csv(os.path.join(cfg.out_dir, "users.csv"), index=False)
    sessions.to_csv(os.path.join(cfg.out_dir, "sessions.csv"), index=False)
    events.to_csv(os.path.join(cfg.out_dir, "events.csv"), index=False)
    lessons.to_csv(os.path.join(cfg.out_dir, "lessons.csv"), index=False)

    print("\nWrote:")
    print(f" - {os.path.join(cfg.out_dir, 'users.csv')}")
    print(f" - {os.path.join(cfg.out_dir, 'sessions.csv')}")
    print(f" - {os.path.join(cfg.out_dir, 'events.csv')}")
    print(f" - {os.path.join(cfg.out_dir, 'lessons.csv')}")


if __name__ == "__main__":
    main()
