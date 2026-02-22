-- =============================================================================
-- Table  : dim_uom
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model (Business Conform Layer)
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per unit of measure
-- Purpose: Reference dimension for units of measure used in specification
--          limits, test results, and material attributes.
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_uom
(
    uom_key                     BIGINT          NOT NULL    GENERATED ALWAYS AS IDENTITY
                                                            COMMENT 'Surrogate primary key',

    uom_code                    STRING          NOT NULL    COMMENT 'Unit code used in data (e.g., %, mg, mg/mL, ppm, CFU/mL)',
    uom_name                    STRING          NOT NULL    COMMENT 'Full unit name (e.g., Percent, Milligram, Milligram per Milliliter)',

    -- uom_category values:
    --   RATIO          = Percent, ppm, ppb
    --   MASS           = mg, g, kg, µg, ng
    --   CONCENTRATION  = mg/mL, µg/mL, mg/L, %w/v
    --   VOLUME         = mL, L, µL
    --   COUNT          = CFU/mL, CFU/g, EU/mL (endotoxin units)
    --   PRESSURE       = bar, Pa, psi
    --   TIME           = min, h, s
    --   TEMPERATURE    = °C, °F, K
    --   DIMENSIONLESS  = Pass/Fail, Conforms, NVT (no visible turbidity)
    --   OTHER          = Specific activity, NTU, cP, mPa·s
    uom_category                STRING                      COMMENT 'Category: RATIO|MASS|CONCENTRATION|VOLUME|COUNT|PRESSURE|TIME|TEMPERATURE|DIMENSIONLESS|OTHER',

    si_conversion_factor        DECIMAL(20, 10)             COMMENT 'Multiplication factor to convert to SI base unit',
    si_uom_code                 STRING                      COMMENT 'SI base unit code for this unit category',

    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL batch load timestamp (UTC)'
)
USING DELTA
COMMENT 'L2.2 Unit of measure reference dimension. Used by specification items and limits. Includes common pharmaceutical units: %, mg, mg/mL, ppm, CFU, EU, and more.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference',
    'quality.grain'                     = 'unit_of_measure'
);

-- =============================================================================
-- SEED DATA — Common Pharmaceutical Units of Measure
-- =============================================================================

INSERT INTO l2_2_spec_unified.dim_uom
    (uom_code, uom_name, uom_category, si_conversion_factor, si_uom_code, load_timestamp)
VALUES
    -- Ratio / Dimensionless (%)
    ('%',           'Percent',                          'RATIO',            0.01,           'fraction',     current_timestamp()),
    ('% w/w',       'Percent Weight/Weight',            'RATIO',            0.01,           'fraction',     current_timestamp()),
    ('% w/v',       'Percent Weight/Volume',            'CONCENTRATION',    10.0,           'g/L',          current_timestamp()),
    ('ppm',         'Parts Per Million',                'RATIO',            0.000001,       'fraction',     current_timestamp()),
    ('ppb',         'Parts Per Billion',                'RATIO',            0.000000001,    'fraction',     current_timestamp()),

    -- Mass
    ('kg',          'Kilogram',                         'MASS',             1.0,            'kg',           current_timestamp()),
    ('g',           'Gram',                             'MASS',             0.001,          'kg',           current_timestamp()),
    ('mg',          'Milligram',                        'MASS',             0.000001,       'kg',           current_timestamp()),
    ('µg',          'Microgram',                        'MASS',             0.000000001,    'kg',           current_timestamp()),
    ('ng',          'Nanogram',                         'MASS',             0.000000000001, 'kg',           current_timestamp()),

    -- Concentration
    ('mg/mL',       'Milligram per Milliliter',         'CONCENTRATION',    1.0,            'g/L',          current_timestamp()),
    ('µg/mL',       'Microgram per Milliliter',         'CONCENTRATION',    0.001,          'g/L',          current_timestamp()),
    ('mg/L',        'Milligram per Liter',              'CONCENTRATION',    0.001,          'g/L',          current_timestamp()),
    ('mg/g',        'Milligram per Gram',               'CONCENTRATION',    0.001,          'g/g',          current_timestamp()),

    -- Volume
    ('L',           'Liter',                            'VOLUME',           0.001,          'm3',           current_timestamp()),
    ('mL',          'Milliliter',                       'VOLUME',           0.000001,       'm3',           current_timestamp()),
    ('µL',          'Microliter',                       'VOLUME',           0.000000001,    'm3',           current_timestamp()),

    -- Microbiological Count
    ('CFU/mL',      'Colony Forming Units per mL',      'COUNT',            NULL,           NULL,           current_timestamp()),
    ('CFU/g',       'Colony Forming Units per Gram',    'COUNT',            NULL,           NULL,           current_timestamp()),
    ('CFU/cm2',     'Colony Forming Units per cm²',     'COUNT',            NULL,           NULL,           current_timestamp()),

    -- Endotoxin
    ('EU/mL',       'Endotoxin Units per mL',           'COUNT',            NULL,           NULL,           current_timestamp()),
    ('EU/mg',       'Endotoxin Units per mg',           'COUNT',            NULL,           NULL,           current_timestamp()),

    -- Time
    ('min',         'Minute',                           'TIME',             60.0,           's',            current_timestamp()),
    ('h',           'Hour',                             'TIME',             3600.0,         's',            current_timestamp()),

    -- Temperature
    ('°C',          'Degrees Celsius',                  'TEMPERATURE',      NULL,           '°C',           current_timestamp()),

    -- Physical
    ('NTU',         'Nephelometric Turbidity Units',    'OTHER',            NULL,           NULL,           current_timestamp()),
    ('cP',          'Centipoise',                       'OTHER',            0.001,          'Pa·s',         current_timestamp()),
    ('mPa·s',       'Millipascal-second',               'OTHER',            0.001,          'Pa·s',         current_timestamp()),
    ('N',           'Newton (Tablet Hardness)',         'OTHER',            1.0,            'N',            current_timestamp()),

    -- Dimensionless / Qualitative
    ('PASS_FAIL',   'Pass / Fail',                      'DIMENSIONLESS',    NULL,           NULL,           current_timestamp()),
    ('CONFORMS',    'Conforms to Description',          'DIMENSIONLESS',    NULL,           NULL,           current_timestamp()),
    ('NONE',        'No Unit (Report Only)',             'DIMENSIONLESS',    NULL,           NULL,           current_timestamp());
