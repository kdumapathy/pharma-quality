-- =============================================================================
-- dim_date population
-- =============================================================================
-- Source : SEQUENCE generator (no source table — pure calendar arithmetic)
-- Target : l2_2_spec_unified.dim_date
-- Grain  : One row per calendar date (1990-01-01 to 2100-12-31)
-- Strategy: INSERT IF EMPTY — skip if already loaded (idempotent guard)
-- Note   : fiscal_year/quarter/month default to calendar year (adjust per company)
--          is_holiday defaults FALSE — no US holiday logic included in base load
-- =============================================================================

USE CATALOG pharma_quality;

INSERT INTO l2_2_spec_unified.dim_date
SELECT
    CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT)                             AS date_key,
    d                                                                   AS full_date,
    YEAR(d)                                                             AS year,
    QUARTER(d)                                                          AS quarter,
    MONTH(d)                                                            AS month,
    DATE_FORMAT(d, 'MMMM')                                             AS month_name,
    DATE_FORMAT(d, 'MMM')                                              AS month_name_short,
    WEEKOFYEAR(d)                                                       AS week_of_year,
    DAYOFYEAR(d)                                                        AS day_of_year,
    DAYOFMONTH(d)                                                       AS day_of_month,
    DAYOFWEEK(d)                                                        AS day_of_week,
    DATE_FORMAT(d, 'EEEE')                                              AS day_name,
    DATE_FORMAT(d, 'EEE')                                              AS day_name_short,
    DATE_FORMAT(d, 'yyyy-MM')                                           AS year_month,
    CONCAT(YEAR(d), '-Q', QUARTER(d))                                   AS year_quarter,
    YEAR(d)                                                             AS fiscal_year,
    QUARTER(d)                                                          AS fiscal_quarter,
    MONTH(d)                                                            AS fiscal_month,
    DAYOFWEEK(d) IN (1, 7)                                              AS is_weekend,
    DAYOFMONTH(d) = 1                                                   AS is_month_start,
    DAYOFMONTH(d) = DAYOFMONTH(LAST_DAY(d))                            AS is_month_end,
    (DAYOFMONTH(d) = 1 AND MONTH(d) IN (1, 4, 7, 10))                  AS is_quarter_start,
    (DAYOFMONTH(d) = DAYOFMONTH(LAST_DAY(d)) AND MONTH(d) IN (3, 6, 9, 12)) AS is_quarter_end,
    (DAYOFMONTH(d) = 1 AND MONTH(d) = 1)                               AS is_year_start,
    (DAYOFMONTH(d) = 31 AND MONTH(d) = 12)                             AS is_year_end,
    FALSE                                                               AS is_holiday,
    NULL                                                                AS holiday_name
FROM (
    SELECT EXPLODE(SEQUENCE(DATE('1990-01-01'), DATE('2100-12-31'), INTERVAL 1 DAY)) AS d
)
WHERE NOT EXISTS (SELECT 1 FROM l2_2_spec_unified.dim_date LIMIT 1);

-- =============================================================================
-- Validation
-- =============================================================================
SELECT
    COUNT(*)                            AS total_rows,
    MIN(full_date)                      AS first_date,
    MAX(full_date)                      AS last_date,
    SUM(CASE WHEN is_weekend   THEN 1 ELSE 0 END) AS weekend_days,
    SUM(CASE WHEN is_year_start THEN 1 ELSE 0 END) AS year_starts,
    SUM(CASE WHEN is_month_end  THEN 1 ELSE 0 END) AS month_ends
FROM l2_2_spec_unified.dim_date;
