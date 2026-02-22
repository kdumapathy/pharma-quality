-- =============================================================================
-- Table  : dim_date
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model (Business Conform Layer)
-- Domain : Shared / Common (used across all quality domains)
-- Grain  : One row per calendar date
-- Purpose: Standard date dimension. Used for effective dates, approval dates,
--          expiry dates in specification dimensions.
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_date
(
    date_key                    INT             NOT NULL    COMMENT 'Surrogate key as integer YYYYMMDD (e.g., 20240115)',
    full_date                   DATE            NOT NULL    COMMENT 'Calendar date value',

    -- Calendar attributes
    year                        INT                         COMMENT 'Calendar year (e.g., 2024)',
    quarter                     INT                         COMMENT 'Calendar quarter (1–4)',
    month                       INT                         COMMENT 'Calendar month (1–12)',
    month_name                  STRING                      COMMENT 'Month name (January–December)',
    month_name_short            STRING                      COMMENT 'Short month name (Jan–Dec)',
    week_of_year                INT                         COMMENT 'ISO week number (1–53)',
    day_of_year                 INT                         COMMENT 'Day of year (1–366)',
    day_of_month                INT                         COMMENT 'Day of month (1–31)',
    day_of_week                 INT                         COMMENT 'Day of week (1=Monday, 7=Sunday, ISO 8601)',
    day_name                    STRING                      COMMENT 'Day name (Monday–Sunday)',
    day_name_short              STRING                      COMMENT 'Short day name (Mon–Sun)',

    -- Year-Month helper for aggregation
    year_month                  STRING                      COMMENT 'YYYY-MM string for monthly grouping',
    year_quarter                STRING                      COMMENT 'YYYY-Q# string for quarterly grouping (e.g., 2024-Q1)',

    -- Fiscal calendar (configure fiscal year start month per company)
    fiscal_year                 INT                         COMMENT 'Fiscal year (adjust based on company FY start)',
    fiscal_quarter              INT                         COMMENT 'Fiscal quarter (1–4)',
    fiscal_month                INT                         COMMENT 'Fiscal month (1–12)',

    -- Flags
    is_weekend                  BOOLEAN                     COMMENT 'TRUE if Saturday or Sunday',
    is_month_start              BOOLEAN                     COMMENT 'TRUE if first day of month',
    is_month_end                BOOLEAN                     COMMENT 'TRUE if last day of month',
    is_quarter_start            BOOLEAN                     COMMENT 'TRUE if first day of quarter',
    is_quarter_end              BOOLEAN                     COMMENT 'TRUE if last day of quarter',
    is_year_start               BOOLEAN                     COMMENT 'TRUE if first day of year (January 1)',
    is_year_end                 BOOLEAN                     COMMENT 'TRUE if last day of year (December 31)',
    is_holiday                  BOOLEAN                     COMMENT 'TRUE if public holiday (US by default; configure per region)',
    holiday_name                STRING                      COMMENT 'Holiday name if is_holiday = TRUE'
)
USING DELTA
COMMENT 'L2.2 Standard date dimension. One row per calendar date. Used for all date foreign keys in specification dimensions. Covers 1990-01-01 to 2100-12-31.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'shared',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference',
    'quality.grain'                     = 'calendar_date'
);

-- =============================================================================
-- Population Query (run once to generate dates from 1990 to 2100)
-- =============================================================================
-- INSERT INTO l2_2_spec_unified.dim_date
-- SELECT
--     CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT)         AS date_key,
--     d                                               AS full_date,
--     YEAR(d)                                         AS year,
--     QUARTER(d)                                      AS quarter,
--     MONTH(d)                                        AS month,
--     DATE_FORMAT(d, 'MMMM')                         AS month_name,
--     DATE_FORMAT(d, 'MMM')                          AS month_name_short,
--     WEEKOFYEAR(d)                                   AS week_of_year,
--     DAYOFYEAR(d)                                    AS day_of_year,
--     DAYOFMONTH(d)                                   AS day_of_month,
--     DAYOFWEEK(d)                                    AS day_of_week,
--     DATE_FORMAT(d, 'EEEE')                          AS day_name,
--     DATE_FORMAT(d, 'EEE')                           AS day_name_short,
--     DATE_FORMAT(d, 'yyyy-MM')                       AS year_month,
--     CONCAT(YEAR(d), '-Q', QUARTER(d))               AS year_quarter,
--     YEAR(d)                                         AS fiscal_year,   -- adjust
--     QUARTER(d)                                      AS fiscal_quarter,
--     MONTH(d)                                        AS fiscal_month,
--     DAYOFWEEK(d) IN (1, 7)                          AS is_weekend,
--     DAYOFMONTH(d) = 1                               AS is_month_start,
--     DAYOFMONTH(d) = DAYOFMONTH(LAST_DAY(d))        AS is_month_end,
--     (DAYOFMONTH(d) = 1 AND MONTH(d) IN (1,4,7,10)) AS is_quarter_start,
--     (DAYOFMONTH(d) = DAYOFMONTH(LAST_DAY(d)) AND MONTH(d) IN (3,6,9,12)) AS is_quarter_end,
--     (DAYOFMONTH(d) = 1 AND MONTH(d) = 1)           AS is_year_start,
--     (DAYOFMONTH(d) = 31 AND MONTH(d) = 12)         AS is_year_end,
--     FALSE                                           AS is_holiday,
--     NULL                                            AS holiday_name
-- FROM (
--     SELECT EXPLODE(SEQUENCE(DATE('1990-01-01'), DATE('2100-12-31'), INTERVAL 1 DAY)) AS d
-- );
