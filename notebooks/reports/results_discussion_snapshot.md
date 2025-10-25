# Results & Discussion — Snapshot

## Volumes & Retention
- Bronze rows: **38,753,035**
- Silver rows: **36,970,717** (Yellow: **36,359,692**; Green: **611,025**)
- Retention (Bronze→Silver): **95.4%**

## Gold Tables
- `trip_features`: **36,970,717**
- `agg_zone_hour`: **1,104,758**
- `agg_od_hour`: **13,931,433**

## Demand Patterns
- `reports/fig_hourly_demand.png` (AM/PM peaks, midday trough; Yellow dominates).
- `reports/table_top10_pickup_zones.csv` (top pickup zones by trips).

## Travel Dynamics
- `reports/fig_speed_by_hour.png` (speed dips at peaks, recovers overnight).

## Lambda vs Kappa (Correctness)
- Overlap keys: **41,877**
- Exact matches: **40,637** (97.04%)
- MAE (trips/key): **0.0348**