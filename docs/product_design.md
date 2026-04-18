# Dashboard Plan

This project will use **three dashboards**.  
The goal is to separate the main product questions into distinct views instead of combining everything into one large dashboard.

---

## 1. Growth Overview Dashboard

### Purpose
Track early-stage product growth, activation, and retention.

### Main questions
- Are users signing up consistently?
- Are new users becoming active?
- Is early retention improving?
- Are users entering healthy active states?

### Core components

#### KPI row
- Daily Signups
- DAU
- WAU
- MAU
- DAU / MAU Ratio
- D1 Retention
- D7 Retention
- D30 Retention

#### Main charts
- Daily signups trend
- DAU / WAU / MAU trend
- Activation funnel:
  - Signup
  - App Open
  - Lesson Started
  - Lesson Completed
- Retention trend or cohort view for D1 / D7 / D30
- Lifecycle active mix:
  - New
  - Current
  - Reactivated
  - Resurrected

### Suggested filters
- Date range
- Signup channel
- Country
- Device OS
- Language target
- Variant

---

## 2. Mature Product Health Dashboard

### Purpose
Track ongoing engagement quality, retention stability, and user health in a more mature product stage.

### Main questions
- Are active users engaging deeply enough?
- Is the product retaining users over time?
- Are more users becoming at risk or dormant?
- Are premium-related metrics improving?

### Core components

#### KPI row
- WAU
- MAU
- DAU / MAU Ratio
- Sessions per Active User
- Average Session Duration
- Lessons per Session
- XP per Active User
- Premium Share or Premium Conversion Rate

#### Main charts
- Sessions per user trend
- Average session duration trend
- Lessons per session trend
- Lifecycle health mix:
  - Current
  - At Risk WAU
  - At Risk MAU
  - Dormant
- Retention trend
- Premium metrics:
  - Paywall shown rate
  - Purchase conversion rate
  - Premium share over time

### Suggested filters
- Date range
- Premium status
- Country
- Device OS
- Language target
- Signup channel
- Variant

---

## 3. A/B Test Dashboard

### Purpose
Track treatment vs control performance and measure experiment impact over time.

### Main questions
- Is treatment outperforming control?
- When does the treatment effect appear?
- Does the effect fade over time?
- Which product metrics move the most?
- Are there any negative guardrail effects?

### Core components

#### KPI row
Show control, treatment, and lift for:
- DAU
- WAU
- MAU
- D1 Retention
- D7 Retention
- D30 Retention
- Sessions per Active User
- Lessons Completed per Active User
- Average Streak Length

#### Main charts
- DAU / WAU / MAU by variant
- Retention by variant
- Funnel conversion by variant:
  - Signup → App Open
  - App Open → Lesson Started
  - Lesson Started → Lesson Completed
- Streak metrics by variant
- Guardrail metrics by variant:
  - Average session duration
  - Lesson completion rate
  - Hearts lost
  - Purchase conversion rate

### Suggested filters
- Experiment date range
- Country
- Device OS
- Signup channel
- Language target
- User cohort
- Premium status

---

# Phase 1 Scope Summary

## Dashboard 1 — Growth Overview
Include:
- KPI row
- Signups trend
- DAU / WAU / MAU trend
- Activation funnel
- D1 / D7 / D30 retention
- Lifecycle active mix

## Dashboard 2 — Mature Product Health
Include:
- KPI row
- Sessions per user
- Average session duration
- Lessons per session
- Lifecycle risk mix
- Retention trend
- Premium metrics

## Dashboard 3 — A/B Test
Include:
- KPI row with lift
- DAU / WAU / MAU by variant
- Retention by variant
- Funnel by variant
- Streak metrics by variant
- Guardrail metrics

---

# Notes

- The three dashboards will use the same underlying dataset.
- The difference is in the **business focus** of each dashboard.
- Dashboard 1 emphasizes acquisition and activation.
- Dashboard 2 emphasizes engagement quality and long-term health.
- Dashboard 3 emphasizes causal experiment measurement.

---

