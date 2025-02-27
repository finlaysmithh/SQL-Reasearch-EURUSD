![Language: SQL](https://img.shields.io/badge/Language-SQL-blue)

-- ====================================================
-- Permanent New Week Opening Gap Fill Analysis for 20+ Pip Gaps (4-Week Check)
-- ====================================================
-- This script:
-- 1. Computes the new week opening gap (current week open minus previous week close)
--    and converts it into pips (1 pip = 0.0001 for EUR/USD).
-- 2. Filters for gap candidates with an absolute gap (in pips) of at least 20.
-- 3. Determines if the gap is filled in:
--      - Week 1: current week (using daily data from that week)
--      - Week 2: the week immediately after the current week
--      - Week 3: two weeks after the current week
--      - Week 4: three weeks after the current week
--    For a positive gap, a fill is when any day's low is <= previous week's close.
--    For a negative gap, a fill is when any day's high is >= previous week's close.
-- 4. Computes an overall fill flag: 1 if the gap is filled in any of weeks 1–4, otherwise 0.
-- 5. Aggregates overall statistics.
--
-- Drop the permanent table if it already exists
DROP TABLE IF EXISTS gap_fills;

-- Create the permanent table with gap fill data
CREATE TABLE gap_fills AS
WITH weekly_gap AS (
  SELECT
    curr.week_start,
    prev.weekly_close AS prev_weekly_close,
    curr.weekly_open,
    curr.weekly_low,
    curr.weekly_high,
    curr.weekly_open - prev.weekly_close AS gap
  FROM eurusd_weekly_analysis curr
  JOIN eurusd_weekly_analysis prev 
    ON curr.week_start = prev.week_start + interval '7 day'
  -- Optionally, restrict to a specific date range:
  -- WHERE curr.week_start >= '2020-01-01'
),
weekly_gap_with_pips AS (
  SELECT *,
         CASE 
           WHEN gap >= 0 THEN gap / 0.0001
           ELSE ABS(gap) / 0.0001
         END AS gap_in_pips
  FROM weekly_gap
),
gap_fills_cte AS (
  SELECT
    w.week_start,
    w.prev_weekly_close,
    w.weekly_open,
    w.weekly_low,
    w.weekly_high,
    w.gap,
    w.gap_in_pips,
    -- Fill in week 1:
    CASE 
      WHEN w.gap >= 0 THEN (
         SELECT d.date
         FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start
           AND d.low_price <= w.prev_weekly_close
         ORDER BY d.date ASC
         LIMIT 1
      )
      ELSE (
         SELECT d.date
         FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start
           AND d.high_price >= w.prev_weekly_close
         ORDER BY d.date ASC
         LIMIT 1
      )
    END AS fill_date_week1,
    CASE 
      WHEN w.gap >= 0 AND w.weekly_low <= w.prev_weekly_close THEN 1
      WHEN w.gap < 0 AND w.weekly_high >= w.prev_weekly_close THEN 1
      ELSE 0
    END AS gap_filled_week1,
    -- Fill in week 2:
    CASE 
      WHEN w.gap >= 0 THEN (
         SELECT d.date
         FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '7 day'
           AND d.low_price <= w.prev_weekly_close
         ORDER BY d.date ASC
         LIMIT 1
      )
      ELSE (
         SELECT d.date
         FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '7 day'
           AND d.high_price >= w.prev_weekly_close
         ORDER BY d.date ASC
         LIMIT 1
      )
    END AS fill_date_week2,
    CASE 
      WHEN w.gap >= 0 AND EXISTS (
         SELECT 1 FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '7 day'
           AND d.low_price <= w.prev_weekly_close
         LIMIT 1
      ) THEN 1
      WHEN w.gap < 0 AND EXISTS (
         SELECT 1 FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '7 day'
           AND d.high_price >= w.prev_weekly_close
         LIMIT 1
      ) THEN 1
      ELSE 0
    END AS gap_filled_week2,
    -- Fill in week 3:
    CASE 
      WHEN w.gap >= 0 THEN (
         SELECT d.date
         FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '14 day'
           AND d.low_price <= w.prev_weekly_close
         ORDER BY d.date ASC
         LIMIT 1
      )
      ELSE (
         SELECT d.date
         FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '14 day'
           AND d.high_price >= w.prev_weekly_close
         ORDER BY d.date ASC
         LIMIT 1
      )
    END AS fill_date_week3,
    CASE 
      WHEN w.gap >= 0 AND EXISTS (
         SELECT 1 FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '14 day'
           AND d.low_price <= w.prev_weekly_close
         LIMIT 1
      ) THEN 1
      WHEN w.gap < 0 AND EXISTS (
         SELECT 1 FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '14 day'
           AND d.high_price >= w.prev_weekly_close
         LIMIT 1
      ) THEN 1
      ELSE 0
    END AS gap_filled_week3,
    -- Fill in week 4:
    CASE 
      WHEN w.gap >= 0 THEN (
         SELECT d.date
         FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '21 day'
           AND d.low_price <= w.prev_weekly_close
         ORDER BY d.date ASC
         LIMIT 1
      )
      ELSE (
         SELECT d.date
         FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '21 day'
           AND d.high_price >= w.prev_weekly_close
         ORDER BY d.date ASC
         LIMIT 1
      )
    END AS fill_date_week4,
    CASE 
      WHEN w.gap >= 0 AND EXISTS (
         SELECT 1 FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '21 day'
           AND d.low_price <= w.prev_weekly_close
         LIMIT 1
      ) THEN 1
      WHEN w.gap < 0 AND EXISTS (
         SELECT 1 FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE = w.week_start + interval '21 day'
           AND d.high_price >= w.prev_weekly_close
         LIMIT 1
      ) THEN 1
      ELSE 0
    END AS gap_filled_week4,
    -- Overall fill: if filled in any week (week 1, 2, 3, or 4) then overall_filled = 1.
    CASE 
      WHEN (
        CASE 
          WHEN w.gap >= 0 AND w.weekly_low <= w.prev_weekly_close THEN 1
          WHEN w.gap < 0 AND w.weekly_high >= w.prev_weekly_close THEN 1
          ELSE 0
        END
      ) = 1 THEN 1
      WHEN EXISTS (
         SELECT 1 FROM eurusd_daily d
         WHERE DATE_TRUNC('week', d.date)::DATE IN (
           w.week_start + interval '7 day',
           w.week_start + interval '14 day',
           w.week_start + interval '21 day'
         )
         AND (
           (w.gap >= 0 AND d.low_price <= w.prev_weekly_close)
           OR (w.gap < 0 AND d.high_price >= w.prev_weekly_close)
         )
         LIMIT 1
      ) THEN 1
      ELSE 0
    END AS overall_filled
  FROM weekly_gap_with_pips w
  WHERE w.gap_in_pips >= 20
)
SELECT * FROM gap_fills_cte;

-- ====================================================
-- Overall Fill Percentages for 20+ Pip Gaps (4-Week Check)
-- ====================================================
SELECT 
    COUNT(*) AS total_gap_candidates,
    SUM(gap_filled_week1) AS filled_week1,
    (COUNT(*) - SUM(gap_filled_week1)) AS not_filled_week1,
    (SUM(gap_filled_week1) * 100.0 / COUNT(*)) AS percent_filled_week1,
    SUM(overall_filled) AS overall_filled,
    (SUM(overall_filled) * 100.0 / COUNT(*)) AS overall_percent_filled,
    ((COUNT(*) - SUM(overall_filled)) * 100.0 / COUNT(*)) AS overall_percent_not_filled
FROM gap_fills;
