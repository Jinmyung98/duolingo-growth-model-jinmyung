# Dashboard Chart Guide

Business logic and rationale behind each chart across all three dashboards.

---

## Dashboard 1 — Growth Overview

**Purpose:** Understand whether the product is acquiring users and turning them into active, retained users. This dashboard is oriented around top-of-funnel health and early retention signals.

---

### KPI Row

#### Daily Signups
How many new users registered today. The most direct measure of acquisition health. A drop here usually points to a marketing or channel problem before it shows up anywhere else.

#### DAU (Daily Active Users)
The number of distinct users active on a given day. Defined as users with at least one `app_open`, `lesson_started`, or `lesson_completed` event. DAU is the core pulse of the product — it reflects whether the user base is actually using the product day to day.

#### WAU (Weekly Active Users)
Distinct users active in the trailing 7 days. Less noisy than DAU for spotting weekly trends since it smooths out day-of-week effects (e.g. weekends tend to have different engagement patterns).

#### MAU (Monthly Active Users)
Distinct users active in the trailing 30 days. The broadest active user count. A large gap between MAU and DAU indicates many users only show up occasionally.

#### DAU / MAU Ratio
The share of monthly actives who are also daily actives. A higher ratio means users are habitual — they come back most days. A low ratio means the product has irregular usage. For a habit-forming product like Duolingo, this ratio is a key health signal.

#### D1 Retention
The share of users who were active exactly 1 day after signup. D1 is the sharpest indicator of first-session quality — if users do not return after day one, no downstream engagement metric can recover.

#### D7 Retention
The share of users still active 7 days after signup. Measures whether the initial habit has started to form after the first week.

#### D30 Retention
The share of users still active 30 days after signup. The clearest signal of whether the product has a sustainable retention loop. Low D30 means the product is not building durable habits regardless of how strong D1 looks.

---

### Daily Signups Trend

**Chart type:** Line Chart | **Source:** `agg_daily_kpis`

Shows how acquisition volume changes over time. Useful for spotting the effect of marketing campaigns, seasonality, or channel changes. When combined with the DAU/MAU trend, it separates whether growth is driven by new signups or improving retention.

---

### DAU / WAU / MAU Trend

**Chart type:** Line Chart | **Source:** `agg_daily_kpis`

Plots all three active user counts on the same axis over time. The relative spread between the three lines tells you about usage frequency. A healthy growing product shows all three lines moving upward together with DAU/MAU ratio staying stable or improving. If MAU grows but DAU stays flat, the product is acquiring users who do not return regularly.

---

### Retention Trend (D1 / D7 / D30)

**Chart type:** Line Chart | **Source:** `agg_retention_cohort`

Shows how retention rates for each cohort change over calendar time. Each point on the line represents a different signup cohort measured at day 1, 7, or 30. This answers whether the product is getting better at retaining users over time — not just whether retained users exist today, but whether newer cohorts retain better than older ones. An upward trend in D30 retention is one of the strongest signs of product improvement.

---

### Lifecycle Active Mix

**Chart type:** Stacked Bar or Stacked Area | **Source:** `agg_lifecycle_daily`

Breaks down DAU each day into four active lifecycle states:

- **New** — active on their signup date. Driven purely by acquisition volume.
- **Current** — active today and was also active in the prior 7 days. The healthiest and most stable active state.
- **Reactivated** — active today after being inactive for 7 days but active within the last 30. Signals re-engagement working but also that users lapsed.
- **Resurrected** — active today after 30+ days of inactivity. Mostly driven by notifications or seasonal events.

A healthy product has Current dominating the mix. If New users are a large share of DAU every day, the product is on a treadmill — constantly acquiring to replace users who leave. If Resurrected is rising, it may mean the product is over-relying on win-back campaigns rather than everyday habit.

---

## Dashboard 2 — Mature Product Health

**Purpose:** Go beyond whether users are active and ask how deeply they engage, whether engagement quality is holding up, and whether the product is moving users toward premium conversion. This dashboard is most relevant once the user base has stabilized and the question shifts from growth to depth.

---

### KPI Row

#### WAU and MAU
Same definitions as Dashboard 1 but here the focus is on stability rather than growth. The question is whether the engaged base is holding steady, not whether it is climbing rapidly.

#### DAU / MAU Ratio
In the mature health context, this ratio is a signal of habit strength. A declining ratio in a mature product is an early warning of engagement erosion even if absolute MAU looks fine.

#### Sessions per Active User
Average number of sessions per DAU per day. Measures how often active users return within a single day. A drop here means users are coming to the app but leaving after one session rather than returning throughout the day.

#### Average Session Duration
Average duration of a single session in seconds. Reflects how deeply users engage per visit. A shortening session duration can mean lessons are getting completed faster (positive) or that users are abandoning sessions earlier (negative) — interpret alongside lesson completion rate.

#### Lessons per Session
Average lessons completed in a session. Measures engagement depth within a single session. This is one of the best proxies for whether users are getting genuine value from the product per visit.

#### XP per Active User
Total XP earned divided by DAU. XP is the in-product reward for completing learning activities. It is a useful engagement composite that captures both session frequency and depth in a single number.

#### Premium Share
Share of active users who are on a premium plan. Tracks monetization penetration over time. An increasing premium share in a growing user base is a strong signal of both product value and revenue health.

---

### Sessions per User Trend

**Chart type:** Line Chart | **Source:** `agg_daily_kpis`

Tracks daily engagement frequency over time. Useful for spotting if feature changes or notification experiments are causing users to open the app more or fewer times per day.

---

### Average Session Duration Trend

**Chart type:** Line Chart | **Source:** `agg_daily_kpis`

Monitors whether the time users spend per visit is stable. A sustained decline often precedes churn. It is also a key guardrail metric for A/B tests — any experiment that shortens session duration significantly is likely hurting the user experience.

---

### Lessons per Session Trend

**Chart type:** Line Chart | **Source:** virtual dataset on `agg_daily_kpis`

Tracks how many lessons users complete per session. This is the most direct measure of learning depth. If lessons per session drops while session duration stays flat, users may be spending time in non-learning parts of the app (social features, settings, streak repair) rather than actually studying.

---

### Lifecycle Health Mix

**Chart type:** Stacked Bar or Stacked Area | **Source:** `agg_lifecycle_daily`

Unlike the Growth dashboard which shows the active states, this chart focuses on the at-risk and disengaging states:

- **Current** — healthy, habitual users.
- **At Risk WAU** — active in the last 30 days but not the last 7. Starting to slip.
- **At Risk MAU** — active in the last 90 days but not the last 30. Significantly lapsed.
- **Dormant** — no activity in 30+ days. Effectively churned unless reactivated.

A growing At Risk or Dormant share is an early warning of retention deterioration even when overall MAU looks stable. This chart often catches problems 2–4 weeks before they show up in MAU.

---

### Retention Trend (D7 / D30)

**Chart type:** Line Chart | **Source:** `agg_retention_cohort`

Same construction as Dashboard 1 but with emphasis on the longer windows. In a mature product, D7 and D30 retention stability is more actionable than D1, since the onboarding loop is usually already optimized. Declining D30 retention in a mature product is a serious signal requiring investigation into habit loops and notification effectiveness.

---

### Paywall Shown Rate Trend

**Chart type:** Line Chart | **Source:** virtual dataset on `events`

Tracks what share of active users are being shown the premium paywall each day. If this rate drops, the product is surfacing monetization opportunities less frequently — either because of feature changes or because user behavior has shifted away from premium-triggering screens.

---

### Purchase Conversion Trend

**Chart type:** Line Chart | **Source:** virtual dataset on `events`

Among users who were shown the paywall, what share completed a purchase. This isolates the quality of the paywall experience from its volume. A drop in conversion with stable paywall shown rate points to a pricing, messaging, or friction issue rather than a traffic problem.

---

### Premium Share Over Time

**Chart type:** Line Chart | **Source:** `agg_daily_kpis`

Tracks the cumulative share of the active user base on premium plans. Unlike the conversion rate (which measures a single paywall event), premium share reflects the long-term monetization trajectory of the user base.

---

## Dashboard 3 — A/B Test

**Purpose:** Measure the causal impact of the experiment by comparing treatment and control across all major product metrics. The goal is not just to confirm that treatment is better overall but to understand which metrics move, when the effect appears, and whether any guardrail metrics are being harmed.

---

### KPI Table (Control / Treatment / Lift)

**Chart type:** Table | **Source:** pivot virtual dataset on `agg_daily_kpis` and `agg_retention_cohort`

Summarizes the overall experiment result in a single view. Each row shows a key metric with three columns: control value, treatment value, and percentage lift. This is the first chart a stakeholder reads — it answers "did the experiment work?" before diving into the charts below. Lift is color-coded green (positive) or red (negative) to make guardrail violations immediately visible.

---

### DAU / WAU / MAU by Variant

**Chart type:** Line Chart | **Source:** `agg_daily_kpis`

Plots the active user counts separately for each variant over the experiment period. The value over the KPI table is that this chart shows *when* the treatment effect appeared and whether it is stable or fading. A treatment effect that peaks in week 1 and then decays suggests novelty rather than genuine habit improvement.

---

### D1 / D7 / D30 Retention by Variant

**Chart type:** Line Chart | **Source:** `agg_retention_cohort`

Compares retention curves by signup cohort for each variant. Retention is the most important metric for a habit-forming product — a treatment that lifts D30 retention is far more valuable than one that only moves D1. Three separate charts (one per retention window) make it easy to see at which horizon the effect is strongest.

---

### Funnel by Variant

**Chart type:** Grouped Bar Chart | **Source:** `agg_funnel_daily`

Compares funnel step conversion rates between control and treatment. This answers *how* the treatment is working — whether it is improving app opens, lesson starts, or lesson completions. If the treatment lifts DAU but the funnel chart shows no change in lesson completion, the extra activity may be low-quality engagement that does not lead to learning.

---

### Streak Metrics by Variant

**Chart type:** Line Chart | **Source:** virtual dataset on `fact_user_daily`

Tracks average streak length and the share of users maintaining an active streak, split by variant. Streaks are the core habit loop mechanic in a Duolingo-style product. A treatment that lifts streak metrics is directly reinforcing the daily habit. If streaks improve, DAU and D7 retention improvements are likely to be durable.

---

### Guardrail Metrics by Variant

Four Line Charts | **Source:** virtual dataset on `events` and `sessions`

Guardrail metrics are metrics the experiment should *not* harm, even if the primary metrics improve. Each chart compares control vs treatment:

- **Average Session Duration** — a drop in session duration in treatment may mean users are rushing through content rather than engaging deeply.
- **Lesson Completion Rate** — if treatment users start more lessons but complete fewer, engagement quality is declining.
- **Hearts Lost per User** — hearts are lost when answers are wrong. A spike may indicate the treatment is pushing users into content that is too difficult.
- **Purchase Conversion Rate** — the experiment should not cannibalize premium conversion. A drop here is a direct revenue guardrail violation.

A primary metric can only be called a win if no guardrail metric shows a statistically significant decline.
