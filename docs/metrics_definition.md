# Metrics Layer (`src/metrics.py`) — Documentation

This module defines the **single source of truth** for all product/growth metrics computed from the raw fact tables:

- `users.csv` (user-level attributes, signup date, variant, etc.)
- `events.csv` (event-level behavioral logs)
- `sessions.csv` (session-level logs)

The metrics layer serves two goals:

1. **Pin down metric definitions** (so DAU/WAU/retention/funnel mean the same thing everywhere).
2. Provide **reusable functions** returning tidy DataFrames for analysis, plotting, A/B testing, and reporting.

---

## 1. Data Contracts

### 1.1 Required columns

#### `events` table
Minimum required columns (exact names depend on your schema; update if needed):

- `user_id` : unique user identifier
- `event_time` or `timestamp` : event timestamp
- `event_date` : event date (daily granularity; can be derived from timestamp)
- `event_name` : event type label (e.g., `app_open`, `lesson_started`, `lesson_completed`)
- `variant` : experiment assignment (e.g., `control`, `treatment`) *(optional but recommended)*

#### `sessions` table
- `user_id`
- `session_id`
- `session_start_time` or `timestamp`
- `session_date`
- `variant` *(optional but recommended)*

#### `users` table
- `user_id`
- `signup_date`
- `variant` *(recommended for consistent cohorting and A/B splits)*

---

## 2. Shared Definitions

### 2.1 Active event set

Define the set of event types that count as “activity”:

\[
\mathcal{E}_{active} \subseteq \{\text{all event_name}\}
\]

Default (recommended):

\[
\mathcal{E}_{active} = \{\texttt{app_open}, \texttt{lesson_started}, \texttt{lesson_completed}\}
\]

A user \(i\) is **active on day \(t\)** if there exists at least one event on that date belonging to \(\mathcal{E}_{active}\).

---

### 2.2 Activity indicator

\[
A_t(i) =
\begin{cases}
1 & \text{if user } i \text{ is active on day } t \\
0 & \text{otherwise}
\end{cases}
\]

---

### 2.3 Rolling window counts

Weekly activity count for user \(i\) at date \(t\):

\[
W_7(t,i) = \sum_{k=0}^{6} A_{t-k}(i)
\]

Monthly activity count:

\[
W_{30}(t,i) = \sum_{k=0}^{29} A_{t-k}(i)
\]

---

## 3. Lifecycle State Definitions (Optional Metrics)

These definitions are used if you want to compute daily lifecycle decomposition (growth accounting). Let:

\[
S_t(i) \in \{New, Current, Reactivated, Resurrected, AtRiskWAU, AtRiskMAU, Dormant\}
\]

Using the activity indicator and windows:

- **New**: \(t = d_i\) (signup date)
- **Current**: \(A_t(i)=1\) and \(W_7(t-1,i) \ge 1\)
- **Reactivated**: \(A_t(i)=1\), \(W_7(t-1,i)=0\), and \(W_{30}(t-1,i)\ge 1\)
- **Resurrected**: \(A_t(i)=1\) and \(W_{30}(t-1,i)=0\)
- **At Risk WAU**: \(A_t(i)=0\) and \(W_7(t,i)\ge 1\)
- **At Risk MAU**: \(W_7(t,i)=0\) and \(W_{30}(t,i)\ge 1\)
- **Dormant**: \(W_{30}(t,i)=0\)

**Active states**:

\[
\mathcal{S}_{active} = \{Reactivated, New, Resurrected, Current\}
\]

Then DAU can be decomposed as:

\[
DAU(t) = \sum_{s \in \mathcal{S}_{active}} N_s(t)
\quad\text{where}\quad
N_s(t)=|\{i : S_t(i)=s\}|
\]

---

## 4. Function Reference

All functions should:

- be deterministic and side-effect free (no file I/O)
- accept raw DataFrames and configuration arguments
- return **tidy** DataFrames (long-form preferred)
- avoid silently changing definitions

### 4.1 `compute_dau(events, active_event_names=None, group_cols=None)`

**Definition**

\[
DAU(t) = |\{ i : \exists e \in E,\ e.date=t,\ e.user=i,\ e.name\in \mathcal{E}_{active}\}|
\]

**Inputs**
- `events`: DataFrame
- `active_event_names`: list[str] (defaults to `["app_open","lesson_started","lesson_completed"]`)
- `group_cols`: optional list[str] for splits (e.g., `["variant"]`)

**Output**
- DataFrame with columns:
  - `date`
  - `dau`
  - plus any grouping columns (e.g., `variant`)

---

### 4.2 `compute_wau(events, active_event_names=None, group_cols=None)`

**Definition (7-day trailing window)**

\[
WAU(t)=\left|\left\{i:\exists \tau\in[t-6,t]\ \text{s.t.}\ A_\tau(i)=1\right\}\right|
\]

**Output**
- DataFrame with `date`, `wau`, and optional grouping columns.

---

### 4.3 `compute_mau(events, active_event_names=None, group_cols=None)`

**Definition (30-day trailing window)**

\[
MAU(t)=\left|\left\{i:\exists \tau\in[t-29,t]\ \text{s.t.}\ A_\tau(i)=1\right\}\right|
\]

**Output**
- DataFrame with `date`, `mau`, and optional grouping columns.

---

### 4.4 `compute_retention(users, events, n_days, active_event_names=None, by_variant=True)`

**Cohort definition**

\[
C(c)=\{i:d_i=c\}
\]

**Day-n retention for cohort \(c\)**

\[
Retention_n(c)=\frac{|\{i\in C(c):A_{c+n}(i)=1\}|}{|C(c)|}
\]

**Inputs**
- `users`: DataFrame with `user_id`, `signup_date` (and optional `variant`)
- `events`: DataFrame
- `n_days`: int or list[int], e.g. `[1,7,30]`
- `by_variant`: whether to compute retention split by variant

**Output**
Long-form DataFrame with columns:
- `cohort_date`
- `day_n`
- `cohort_size`
- `retained_users`
- `retention_rate`
- optional: `variant`

---

### 4.5 `compute_sessions_per_user(sessions, events=None, active_event_names=None, group_cols=None)`

Recommended definition:

\[
SessionsPerActiveUser(t)=\frac{\#Sessions(t)}{DAU(t)}
\]

**Inputs**
- `sessions`: DataFrame
- `events`: optional DataFrame; if provided, DAU uses `events`, otherwise can estimate active users from `sessions`
- `group_cols`: e.g., `["variant"]`

**Output**
- DataFrame with columns:
  - `date`
  - `sessions`
  - `active_users`
  - `sessions_per_user`

---

### 4.6 `compute_lesson_funnel(events, window=None, group_cols=None)`

A minimal lesson funnel (customize to your product spec):

Let:
- `app_open` = activation signal
- `lesson_started` = intent
- `lesson_completed` = success

For a period \(T\) (e.g., daily or weekly), define unique user counts:

\[
N_{open}(T)=|\{i:\exists e\in T,\ e.name=app\_open\}|
\]
\[
N_{start}(T)=|\{i:\exists e\in T,\ e.name=lesson\_started\}|
\]
\[
N_{complete}(T)=|\{i:\exists e\in T,\ e.name=lesson\_completed\}|
\]

Conversion rates:
\[
CR_{start|open}=\frac{N_{start}}{N_{open}},\quad
CR_{complete|start}=\frac{N_{complete}}{N_{start}}
\]

**Output**
- DataFrame with:
  - time grain columns (e.g., `date` or `week_start`)
  - `n_open`, `n_started`, `n_completed`
  - conversion rates

---

### 4.7 `compute_lifecycle_counts(events, users=None, group_cols=None)` *(growth accounting / optional)*

Computes daily counts in each lifecycle state:

\[
N_s(t)=|\{i:S_t(i)=s\}|
\]

Also returns active decomposition identity:

\[
DAU(t)=N_{New}(t)+N_{Current}(t)+N_{Reactivated}(t)+N_{Resurrected}(t)
\]

**Output**
- DataFrame with columns:
  - `date`
  - `state` (one of the 7 states)
  - `users` (count)
  - optional grouping columns

---

## 5. Output Shape Conventions

To make downstream work easy:

- Daily time series should have a `date` column and one metric column (`dau`, `wau`, `mau`, etc.)
- Cohort metrics should have `cohort_date` and `day_n`
- Funnels should be long-form or wide-form consistently (choose one and stick to it)

Recommended: **long-form** for stacking across variants and time.

---

## 6. Experiment Splits

If `variant` exists, all metrics should support optional splitting:

- `group_cols=["variant"]`
- or a `by_variant=True` argument

This avoids re-implementing A/B logic in notebooks.

---

## 7. Reproducibility Notes

- The metrics layer should not read/write files.
- File I/O lives in notebooks or a separate `build_metrics_tables.py`.
- All definitions should be centralized here to ensure consistency across analyses.

---

## 8. Example Usage (Notebook)

```python
import pandas as pd
from src.metrics import (
    compute_dau, compute_wau, compute_mau,
    compute_retention, compute_sessions_per_user,
    compute_lesson_funnel
)

events = pd.read_csv("data/events.csv", parse_dates=["event_time", "event_date"])
users = pd.read_csv("data/users.csv", parse_dates=["signup_date"])
sessions = pd.read_csv("data/sessions.csv", parse_dates=["session_start_time", "session_date"])

dau = compute_dau(events, group_cols=["variant"])
wau = compute_wau(events, group_cols=["variant"])
ret = compute_retention(users, events, n_days=[1,7,30], by_variant=True)
funnel = compute_lesson_funnel(events, group_cols=["variant"])