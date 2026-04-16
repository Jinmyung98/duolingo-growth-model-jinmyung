# Data Generation & Sanity Check Documentation  
**Duolingo-style Growth Model Simulation**

## 1. Overview

This project simulates user growth and engagement dynamics for a Duolingo-like learning product using:

1. A **7-state Markov model** for user lifecycle transitions  
2. **Time-varying transition matrices** capturing product maturity  
3. A **time-varying A/B experiment** with gradual rollout and fade-out  
4. A **state- and streak-dependent activity model** to generate realistic daily usage  
5. A comprehensive set of **sanity checks** to validate model behavior before large-scale data generation

The simulation runs at **daily granularity** over a 365-day horizon.

---

## 2. User State Space

Each user belongs to exactly one state at any point in time:

| State ID | State Name |
|--------:|-----------|
| 1 | Reactivated Users |
| 2 | New Users |
| 3 | Resurrected Users |
| 4 | Current Users |
| 5 | At Risk WAUs |
| 6 | At Risk MAUs |
| 7 | Dormant Users |

Let  

S_t ∈ {1,2,…,7}

denote a user’s state on day t.

---

## 3. Baseline Transition Dynamics (Startup vs Mature)

Two **row-stochastic** transition matrices are defined:

- P^(0): Startup dynamics  
- P^(1): Mature dynamics  

Each is a 7×7 matrix where:

P_ij = Pr(S_{t+1} = j | S_t = i),   ∑_j P_ij = 1

These matrices encode assumptions such as:
- Higher churn and instability in early product stages
- Stronger retention and recovery in mature stages

---

## 4. Time-Varying Product Maturity

### 4.1 Logistic Maturity Curve

Product maturity evolves smoothly over time using a logistic function:

λ(t) = 1 / (1 + exp(−k(t − t₀)))

Where:
- t = day index
- t₀ = 120 (maturity midpoint)
- k = 0.06 (steepness)

Properties:
- λ(t) ∈ (0,1)
- Slow early growth → fast mid-growth → slow saturation
- Interpretable as “how mature the product is” at time t

### 4.2 Interpolated Baseline Matrix

The baseline transition matrix on day t is:

P_base(t) = (1 − λ(t)) · P^(0) + λ(t) · P^(1)

This ensures:
- Valid probabilities at all times
- Smooth evolution from startup to mature behavior

---

## 5. A/B Experiment with Gradual Rollout (Option 3)

### 5.1 Experiment Rollout Function

The experiment does **not** start at day 0. Instead, it rolls out gradually:

g(t) = 1 / (1 + exp(−k_e(t − t_e)))

Where:
- t_e = 90 (experiment start day)
- k_e = 0.15 (rollout speed)

This models realistic feature launches:
- Near-zero effect before rollout
- Gradual adoption
- Full coverage after rollout

### 5.2 Time-Varying Treatment Effect

The effective treatment scale is:

scale(t) = g(t) · (1 − λ(t))

Interpretation:
- Early product → large treatment impact
- Mature product → diminishing marginal impact

### 5.3 Treatment on Transition Matrices

Treatment modifies **only selected rows** of the baseline matrix:
- New Users (state 2)
- At Risk WAUs (state 5)

Probability mass is shifted from:
- At Risk MAUs (state 6)
- Dormant Users (state 7)

towards:
- Current Users (state 4)

All transformations preserve row stochasticity.

---

## 6. Daily Activity Model (State + Streak)

### 6.1 Base Activity by State

Each state s has a baseline activity probability p₀(s):

0 < p₀(s) < 1

### 6.2 Streak Feedback Mechanism

Let L_t be the current streak length.

logit(p_t) = logit(p₀(S_t)) + α(t) · log(1 + L_t) + u_i + d_t

Where:
- α(t): streak sensitivity
- u_i: user-level random effect (currently 0)
- d_t: day-of-week effect (currently 0)

Final probability:
p_t = sigmoid(logit(p_t))

### 6.3 Treatment Effect on Habit Formation

Treatment increases streak sensitivity:

α_treat(t) = α₀ · (1 + κ · g(t))

Control users always use α₀.

This creates a behavioral mechanism:
treatment → stronger habit loop → higher activity → improved retention

---

## 7. Daily Simulation Order

For each user and each day t:

1. Select transition matrix (control or treatment)
2. Generate activity and update streak
3. Transition to next-day state

---

## 8. Sanity Checks

Before generating large datasets, the following checks are run:

- Transition matrices are row-stochastic
- Maturity λ(t) increases smoothly
- Experiment ramp g(t) turns on around day 90
- Control and treatment are identical pre-rollout
- Treatment effect peaks post-rollout and fades with maturity
- Activity probability increases with streak length
- Treatment increases streak sensitivity only after rollout
- Population-level simulations show plausible lifts in engagement and retention

---

## 9. Interpretation

This framework cleanly separates:

- Structural retention dynamics (Markov transitions)
- Behavioral engagement dynamics (activity + streaks)
- Product maturity effects
- Experiment rollout effects

As a result, downstream analyses (retention curves, A/B tests, causal narratives) behave realistically and remain fully interpretable.
