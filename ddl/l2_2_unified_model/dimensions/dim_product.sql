CREATE OR REPLACE TABLE l2_2_unified_model.dim_product
(
    product_key                 BIGINT          NOT NULL    COMMENT 'Surrogate key',
    product_id                  STRING          NOT NULL    COMMENT 'MDM-resolved product ID',
    product_name                STRING          NOT NULL    COMMENT 'Product name (INN + strength + dosage form)',
    inn_name                    STRING                      COMMENT 'International Nonproprietary Name (WHO INN)',
    brand_name                  STRING                      COMMENT 'Brand / trade name',
    product_family              STRING                      COMMENT 'Product family grouping',
    dosage_form_code            STRING                      COMMENT 'Dosage form code: TAB|CAP|INJ|SOL|SUS|CRM|OIN|PATCH|INH|LYOPH',
    dosage_form_name            STRING                      COMMENT 'Dosage form display name (Film-Coated Tablet, Capsule, etc.)',
    route_of_administration     STRING                      COMMENT 'Route: ORAL|IV|IM|SC|TOPICAL|INHALATION|NASAL|OPHTHALMIC|OTIC',
    strength                    STRING                      COMMENT 'Strength string (e.g., 500 mg, 250 mg/5 mL)',
    strength_value              DECIMAL(12, 4)              COMMENT 'Numeric strength value',
    strength_uom                STRING                      COMMENT 'Strength unit (mg, mg/mL, %, IU)',
    therapeutic_area            STRING                      COMMENT 'Therapeutic area (Oncology, Cardiology, CNS, etc.)',
    nda_number                  STRING                      COMMENT 'NDA/ANDA/BLA/MAA registration number',
    shelf_life_months           INT                         COMMENT 'Approved shelf life in months',
    storage_conditions          STRING                      COMMENT 'Labeled storage conditions (Store below 25°C)',
    container_closure_system    STRING                      COMMENT 'Primary packaging (HDPE bottle, blister, vial)',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active flag',
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'Load timestamp'
)
USING DELTA
COMMENT 'L2.2 MDM: Drug product master dimension (PQ/CMC).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.table_type'                = 'mdm_dimension'
);
