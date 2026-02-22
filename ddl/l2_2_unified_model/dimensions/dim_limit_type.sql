-- =============================================================================
-- Table  : dim_limit_type
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model (Business Conform Layer)
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per limit type (static reference / lookup table)
-- Purpose: Reference dimension classifying each limit value in
--          fact_specification_limit. Enables a single fact table to store all
--          limit types (NOR, PAR, AC, Alert, Action, IPC) without pivoting.
--          This is the key design enabler for the normalized limits model.
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_limit_type
(
    -- -------------------------------------------------------------------------
    -- Keys
    -- -------------------------------------------------------------------------
    limit_type_key              BIGINT          NOT NULL    GENERATED ALWAYS AS IDENTITY
                                                            COMMENT 'Surrogate primary key (system-generated)',

    limit_type_code             STRING          NOT NULL    COMMENT 'Business code: AC|NOR|PAR|ALERT|ACTION|IPC_LIMIT|REPORT|COMPENDIA',

    -- -------------------------------------------------------------------------
    -- Limit Type Description
    -- -------------------------------------------------------------------------
    limit_type_name             STRING          NOT NULL    COMMENT 'Display name (e.g., Acceptance Criteria, Normal Operating Range)',
    limit_type_description      STRING                      COMMENT 'Full description of when and why this limit type is used',

    -- -------------------------------------------------------------------------
    -- Hierarchy
    -- hierarchy_level:
    --   1 = Internal operational (not in regulatory filing)
    --   2 = Site / facility (not in regulatory filing)
    --   3 = Regulatory (appears in CTD / registration filing)
    -- -------------------------------------------------------------------------
    hierarchy_level             INT                         COMMENT 'Hierarchy level: 1=Internal|2=Site|3=Regulatory',

    -- -------------------------------------------------------------------------
    -- Limit Category
    -- limit_category values:
    --   OPERATIONAL    = NOR — day-to-day operational tighter limit
    --   DESIGN_SPACE   = PAR — proven range from development (ICH Q8)
    --   REGULATORY     = AC  — filed/registered specification limit
    --   PROCESS_CTRL   = Alert/Action/IPC — process control limits
    --   INFORMATIONAL  = Report only
    -- -------------------------------------------------------------------------
    limit_category              STRING                      COMMENT 'Category: OPERATIONAL|DESIGN_SPACE|REGULATORY|PROCESS_CTRL|INFORMATIONAL',

    -- -------------------------------------------------------------------------
    -- Regulatory Filing Flag
    -- -------------------------------------------------------------------------
    is_regulatory               BOOLEAN                     COMMENT 'TRUE = this limit type appears in regulatory filings (CTD)',

    -- -------------------------------------------------------------------------
    -- Display
    -- -------------------------------------------------------------------------
    sort_order                  INT                         COMMENT 'Display sort order for reports and UIs',

    -- -------------------------------------------------------------------------
    -- Audit
    -- -------------------------------------------------------------------------
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL batch load timestamp (UTC)'
)
USING DELTA
COMMENT 'L2.2 Limit type reference dimension. Classifies limit values stored in fact_specification_limit: Acceptance Criteria (AC), Normal Operating Range (NOR), Proven Acceptable Range (PAR), Alert, Action, IPC, and Report.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'reference',
    'quality.grain'                     = 'limit_type'
);

-- =============================================================================
-- SEED DATA — Reference values for dim_limit_type
-- These are static business reference values, inserted once.
-- =============================================================================

INSERT INTO l2_2_spec_unified.dim_limit_type
    (limit_type_code, limit_type_name, limit_type_description,
     hierarchy_level, limit_category, is_regulatory, sort_order, load_timestamp)
VALUES
    -- Regulatory / CTD limits
    ('AC',          'Acceptance Criteria',
     'The specification limits as defined in the regulatory filing (CTD 3.2.S.4.1 / 3.2.P.5.1). A batch must meet these limits to be released. Derived from development data, regulatory guidelines, and compendia requirements.',
     3, 'REGULATORY', TRUE, 10, current_timestamp()),

    ('PAR',         'Proven Acceptable Range',
     'The broader range of process parameter(s) within which a product meeting specifications will be produced, as established during development and potentially filed as part of the design space (ICH Q8). May be wider than the Acceptance Criteria.',
     3, 'DESIGN_SPACE', TRUE, 20, current_timestamp()),

    ('COMPENDIA',   'Compendia Limit',
     'Limit as defined by official pharmacopoeia (USP, EP, JP, BP). Applies when the specification references a compendial test without modification.',
     3, 'REGULATORY', TRUE, 30, current_timestamp()),

    -- Internal operational limits
    ('NOR',         'Normal Operating Range',
     'The tighter internal operating range used to consistently manufacture and test within Acceptance Criteria. NOR is not typically filed in the regulatory submission. NOR ≤ AC (always tighter than or equal to AC).',
     1, 'OPERATIONAL', FALSE, 40, current_timestamp()),

    ('ALERT',       'Alert Limit',
     'Internal early-warning limit, tighter than the Action Limit. Crossing this limit triggers review and investigation initiation without mandatory batch rejection. Used for trending and process monitoring.',
     1, 'PROCESS_CTRL', FALSE, 50, current_timestamp()),

    ('ACTION',      'Action Limit',
     'Internal limit that triggers mandatory corrective action when exceeded. Stricter than the Acceptance Criteria. Crossing this limit requires documented investigation and corrective action (CAPA).',
     1, 'PROCESS_CTRL', FALSE, 60, current_timestamp()),

    ('IPC_LIMIT',   'In-Process Control Limit',
     'Limits applied during the manufacturing process at defined control points (e.g., blend uniformity, tablet hardness, in-process dissolution). Ensures the process is under control before final release testing.',
     2, 'PROCESS_CTRL', FALSE, 70, current_timestamp()),

    ('REPORT',      'Report Only',
     'No limit defined; result is measured and reported for data trending and informational purposes only. Typical at early development stages or for tracking purposes.',
     1, 'INFORMATIONAL', FALSE, 80, current_timestamp());

-- =============================================================================
-- Limit Type Hierarchy Rule
-- =============================================================================
-- PAR  >= AC  >= NOR
-- AC   >= ALERT >= ACTION
-- The PAR must encompass the AC; the AC must encompass the NOR.
-- Violation of this hierarchy is a data quality rule to be enforced in L2.2.
