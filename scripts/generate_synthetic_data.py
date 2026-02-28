"""
Synthetic Data Generator for Pharma Quality Unified Data Model (PQ/CMC)

Generates realistic pharmaceutical quality data aligned with ICH guidelines:
- 10K+ records per dimension table
- 100K+ records per fact table

Usage (Databricks):
    %run ./scripts/generate_synthetic_data

Usage (standalone):
    python scripts/generate_synthetic_data.py --output-dir ./synthetic_data
"""

import random
import csv
import os
import hashlib
import argparse
from datetime import datetime, date, timedelta
from decimal import Decimal

# ─────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────

NUM_PRODUCTS = 500
NUM_MATERIALS = 600
NUM_TEST_METHODS = 400
NUM_SITES = 50
NUM_MARKETS = 30
NUM_SPECIFICATIONS = 2000
NUM_SPEC_ITEMS_PER_SPEC = 8  # avg items per spec
NUM_BATCHES = 5000
NUM_INSTRUMENTS = 200
NUM_LABORATORIES = 80
NUM_FACT_SPEC_LIMITS = 120000  # > 100K
NUM_FACT_ANALYTICAL_RESULTS = 150000  # > 100K

# ─────────────────────────────────────────────────
# Reference data
# ─────────────────────────────────────────────────

INN_NAMES = [
    "Acetaminophen", "Ibuprofen", "Metformin", "Atorvastatin", "Amlodipine",
    "Omeprazole", "Losartan", "Levothyroxine", "Lisinopril", "Simvastatin",
    "Azithromycin", "Amoxicillin", "Gabapentin", "Hydrochlorothiazide", "Metoprolol",
    "Sertraline", "Pantoprazole", "Escitalopram", "Montelukast", "Rosuvastatin",
    "Clopidogrel", "Valsartan", "Duloxetine", "Fluoxetine", "Telmisartan",
    "Tamsulosin", "Carvedilol", "Furosemide", "Prednisone", "Dapagliflozin",
    "Empagliflozin", "Sitagliptin", "Rivaroxaban", "Apixaban", "Adalimumab",
    "Pembrolizumab", "Nivolumab", "Trastuzumab", "Rituximab", "Bevacizumab",
    "Cetuximab", "Insulin Glargine", "Insulin Lispro", "Semaglutide", "Dulaglutide",
    "Liraglutide", "Canagliflozin", "Saxagliptin", "Vildagliptin", "Alogliptin",
]

DOSAGE_FORMS = [
    ("TAB", "Film-Coated Tablet"), ("TAB", "Immediate-Release Tablet"),
    ("TAB", "Extended-Release Tablet"), ("TAB", "Chewable Tablet"),
    ("CAP", "Hard Gelatin Capsule"), ("CAP", "Soft Gelatin Capsule"),
    ("INJ", "Solution for Injection"), ("INJ", "Lyophilized Powder for Injection"),
    ("SOL", "Oral Solution"), ("SUS", "Oral Suspension"),
    ("CRM", "Topical Cream"), ("OIN", "Topical Ointment"),
    ("PATCH", "Transdermal Patch"), ("INH", "Metered-Dose Inhaler"),
    ("LYOPH", "Lyophilized Powder"),
]

ROUTES = ["ORAL", "IV", "IM", "SC", "TOPICAL", "INHALATION", "NASAL", "OPHTHALMIC"]
STRENGTHS = ["5 mg", "10 mg", "25 mg", "50 mg", "100 mg", "200 mg", "250 mg", "500 mg",
             "1 g", "2.5 mg/mL", "5 mg/mL", "10 mg/mL", "20 mg/mL", "50 mg/mL",
             "0.5%", "1%", "2%", "5%", "100 IU/mL", "300 IU/mL"]
THERAPEUTIC_AREAS = ["Oncology", "Cardiology", "CNS", "Immunology", "Endocrinology",
                     "Infectious Disease", "Respiratory", "Gastroenterology", "Pain",
                     "Ophthalmology", "Dermatology", "Hematology", "Nephrology"]
STORAGE_CONDITIONS = ["Store below 25C", "Store below 30C", "Store at 2-8C", "Store at -20C",
                      "Protect from light", "Store in dry place"]
CONTAINERS = ["HDPE bottle", "Glass vial", "PVC/Alu blister", "Alu/Alu blister",
              "Prefilled syringe", "Amber glass bottle", "LDPE bag"]

MATERIAL_TYPES = [
    ("API", "Active Pharmaceutical Ingredient"),
    ("EXCIPIENT", "Excipient"),
    ("INTERMEDIATE", "Intermediate"),
    ("PACKAGING", "Packaging Material"),
    ("REFERENCE_STD", "Reference Standard"),
    ("RAW_MATERIAL", "Raw Material"),
]
PHARMACOPOEIA_GRADES = ["USP", "EP", "JP", "NF", "ACS", "IN_HOUSE"]
SUPPLIER_NAMES = ["Sigma-Aldrich", "BASF", "Evonik", "Colorcon", "Ashland",
                  "Shin-Etsu", "DFE Pharma", "Roquette", "Gattefosse", "Croda",
                  "Merck KGaA", "Teva API", "Dr. Reddy's", "Aurobindo", "MSN Labs"]

TEST_CATEGORIES = [
    ("PHY", "Physical"), ("CHE", "Chemical"), ("IMP", "Impurities"),
    ("MIC", "Microbiology"), ("BIO", "Biological"), ("STER", "Sterility"),
    ("PACK", "Packaging"),
]
ANALYTICAL_TECHNIQUES = ["HPLC", "GC", "UV_VIS", "IR", "KF", "DISSOLUTION",
                         "HARDNESS", "PSD", "LAL", "XRPD", "DSC", "TGA", "NMR",
                         "ICP_MS", "ICP_OES", "AAS", "POTENCY", "ELISA"]
CRITICALITIES = ["CQA", "CCQA", "NCQA", "KQA", "REPORT"]
SPEC_TYPES = [("DS", "Drug Substance"), ("DP", "Drug Product"), ("RM", "Raw Material"),
              ("EXCIP", "Excipient"), ("INTERMED", "Intermediate"), ("IPC", "In-Process Control")]
SPEC_STATUSES = [("APP", "Approved"), ("DRA", "Draft"), ("SUP", "Superseded")]
STAGES = [("DEV", "Development"), ("CLI", "Clinical"), ("COM", "Commercial")]
LIMIT_TYPES = ["AC", "NOR", "PAR"]
BATCH_TYPES = ["DEVELOPMENT", "PILOT", "EXHIBIT", "REGISTRATION", "COMMERCIAL", "VALIDATION"]
BATCH_STATUSES = ["RELEASED", "QUARANTINE", "PENDING"]
INSTRUMENT_TYPES = ["HPLC", "GC", "UV_VIS", "IR", "DISSOLUTION", "BALANCE", "PH_METER", "KF", "PSD"]
INSTRUMENT_MANUFACTURERS = ["Agilent", "Waters", "Shimadzu", "Thermo Fisher", "PerkinElmer",
                            "Bruker", "Mettler Toledo", "Sotax", "Malvern"]
QUALIFICATION_STATUSES = ["QUALIFIED", "PENDING_OQ", "PENDING_PQ"]
LAB_TYPES = ["QC", "R_AND_D", "STABILITY", "MICROBIOLOGY", "CRO", "CONTRACT"]
ACCREDITATION_STATUSES = ["ISO_17025", "GLP", "GMP_COMPLIANT", "PENDING"]

COUNTRIES = [
    ("US", "United States"), ("DE", "Germany"), ("JP", "Japan"), ("CN", "China"),
    ("GB", "United Kingdom"), ("FR", "France"), ("IN", "India"), ("BR", "Brazil"),
    ("CH", "Switzerland"), ("IT", "Italy"), ("CA", "Canada"), ("AU", "Australia"),
    ("KR", "South Korea"), ("MX", "Mexico"), ("ES", "Spain"), ("NL", "Netherlands"),
    ("SE", "Sweden"), ("DK", "Denmark"), ("IE", "Ireland"), ("SG", "Singapore"),
    ("IL", "Israel"), ("BE", "Belgium"), ("AT", "Austria"), ("PL", "Poland"),
    ("ZA", "South Africa"), ("AR", "Argentina"), ("TW", "Taiwan"),
    ("MY", "Malaysia"), ("TH", "Thailand"), ("PH", "Philippines"),
]

REGIONS = {"US": "US", "CA": "US", "MX": "US",
           "DE": "EU", "GB": "EU", "FR": "EU", "IT": "EU", "ES": "EU", "NL": "EU",
           "SE": "EU", "DK": "EU", "IE": "EU", "BE": "EU", "AT": "EU", "PL": "EU", "CH": "EU",
           "JP": "JP", "CN": "CN", "KR": "ROW", "TW": "ROW",
           "IN": "ROW", "BR": "ROW", "AU": "ROW", "SG": "ROW",
           "IL": "ROW", "ZA": "ROW", "AR": "ROW", "MY": "ROW", "TH": "ROW", "PH": "ROW"}

REGULATORY_AUTHORITIES = {"US": "FDA", "EU": "EMA", "JP": "PMDA", "CN": "NMPA", "ROW": None}
PHARMACOPOEIAS = {"US": "USP", "EU": "EP", "JP": "JP", "CN": "ChP", "ROW": "USP"}
GMP_STATUSES = ["APPROVED", "PENDING", "APPROVED", "APPROVED"]  # weighted
INSPECTION_OUTCOMES = ["NAI", "VAI", "NAI", "NAI"]  # weighted


def _hash(val):
    return int(hashlib.md5(str(val).encode()).hexdigest()[:15], 16)


def _rand_date(start_year=2020, end_year=2026):
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def _rand_cas():
    p1 = random.randint(50, 999999)
    p2 = random.randint(10, 99)
    p3 = random.randint(0, 9)
    return f"{p1}-{p2}-{p3}"


def _rand_mol_formula():
    elements = ["C", "H", "N", "O", "S", "F", "Cl", "Br", "P"]
    formula = ""
    for e in random.sample(elements, random.randint(2, 5)):
        n = random.randint(1, 30)
        formula += f"{e}{n}" if n > 1 else e
    return formula


# ─────────────────────────────────────────────────
# Generators
# ─────────────────────────────────────────────────

def generate_dim_product(n):
    rows = []
    for i in range(1, n + 1):
        inn = random.choice(INN_NAMES)
        df_code, df_name = random.choice(DOSAGE_FORMS)
        strength = random.choice(STRENGTHS)
        route = random.choice(ROUTES)
        product_id = f"PROD-{i:05d}"
        product_name = f"{inn} {strength} {df_name}"
        rows.append({
            "product_key": _hash(product_id),
            "product_id": product_id,
            "product_name": product_name,
            "inn_name": inn,
            "brand_name": f"{inn[:4].upper()}BRAND-{random.randint(1,99)}" if random.random() > 0.3 else None,
            "product_family": inn,
            "dosage_form_code": df_code,
            "dosage_form_name": df_name,
            "route_of_administration": route,
            "strength": strength,
            "strength_value": float(strength.split()[0]) if strength.split()[0].replace('.','').isdigit() else None,
            "strength_uom": strength.split()[-1] if len(strength.split()) > 1 else "mg",
            "therapeutic_area": random.choice(THERAPEUTIC_AREAS),
            "nda_number": f"NDA-{random.randint(100000, 999999)}" if random.random() > 0.2 else None,
            "shelf_life_months": random.choice([24, 36, 48, 60]),
            "storage_conditions": random.choice(STORAGE_CONDITIONS),
            "container_closure_system": random.choice(CONTAINERS),
            "is_active": True,
            "load_timestamp": datetime.now().isoformat(),
        })
    return rows


def generate_dim_material(n):
    rows = []
    for i in range(1, n + 1):
        mt_code, mt_name = random.choice(MATERIAL_TYPES)
        inn = random.choice(INN_NAMES) if mt_code == "API" else None
        material_id = f"MAT-{i:05d}"
        material_name = inn if inn else f"Material-{i:05d}"
        rows.append({
            "material_key": _hash(material_id),
            "material_id": material_id,
            "material_name": material_name,
            "material_type_code": mt_code,
            "material_type_name": mt_name,
            "cas_number": _rand_cas(),
            "molecular_formula": _rand_mol_formula() if mt_code == "API" else None,
            "molecular_weight": round(random.uniform(100, 1200), 4) if mt_code == "API" else None,
            "inn_name": inn,
            "compendial_name": material_name,
            "pharmacopoeia_grade": random.choice(PHARMACOPOEIA_GRADES),
            "grade": "pharmaceutical",
            "supplier_name": random.choice(SUPPLIER_NAMES),
            "retest_period_months": random.choice([12, 24, 36, 48, 60]),
            "storage_requirements": random.choice(STORAGE_CONDITIONS),
            "is_active": True,
            "load_timestamp": datetime.now().isoformat(),
        })
    return rows


def generate_dim_test_method(n):
    rows = []
    techniques_list = ANALYTICAL_TECHNIQUES
    for i in range(1, n + 1):
        technique = random.choice(techniques_list)
        method_id = f"MTH-{i:05d}"
        rows.append({
            "test_method_key": _hash(method_id),
            "test_method_id": method_id,
            "test_method_name": f"{technique} Method {i}",
            "test_method_number": f"TM-{technique}-{i:04d}",
            "test_method_version": f"{random.randint(1,5)}.0",
            "method_type": random.choice(["COMPENDIAL", "COMPENDIAL_MODIFIED", "IN_HOUSE", "TRANSFER"]),
            "analytical_technique": technique,
            "compendia_reference": f"USP <{random.randint(100,999)}>" if random.random() > 0.4 else None,
            "detection_limit": round(random.uniform(0.001, 0.1), 6) if technique in ["HPLC", "GC", "UV_VIS"] else None,
            "quantitation_limit": round(random.uniform(0.01, 0.5), 6) if technique in ["HPLC", "GC", "UV_VIS"] else None,
            "validation_status": random.choice(["VALIDATED", "VERIFIED", "QUALIFIED", "TRANSFER_COMPLETE"]),
            "validation_date": str(_rand_date(2020, 2025)),
            "is_validated": True,
            "is_active": True,
            "effective_from": str(_rand_date(2020, 2024)),
            "effective_to": None,
            "load_timestamp": datetime.now().isoformat(),
        })
    return rows


def generate_dim_site(n):
    rows = []
    for i in range(1, n + 1):
        cc, cn = random.choice(COUNTRIES)
        region = REGIONS.get(cc, "ROW")
        site_id = f"SITE-{i:04d}"
        rows.append({
            "site_key": _hash(site_id),
            "site_id": site_id,
            "site_code": f"S{cc}-{i:03d}",
            "site_name": f"{cn} Plant {i}",
            "site_type": random.choice(["MANUFACTURING", "QC_TESTING", "PACKAGING", "CMO", "CRO"]),
            "address_line": f"{random.randint(1,999)} Industrial Blvd",
            "city": f"City-{i}",
            "state_province": f"State-{i % 50}",
            "country_code": cc,
            "country_name": cn,
            "regulatory_region": REGULATORY_AUTHORITIES.get(region, "ROW"),
            "gmp_status": random.choice(GMP_STATUSES),
            "gmp_certificate_number": f"GMP-{cc}-{random.randint(10000,99999)}",
            "fda_fei_number": f"FEI-{random.randint(1000000,9999999)}" if cc == "US" else None,
            "last_inspection_date": str(_rand_date(2022, 2025)),
            "last_inspection_outcome": random.choice(INSPECTION_OUTCOMES),
            "is_active": True,
            "load_timestamp": datetime.now().isoformat(),
        })
    return rows


def generate_dim_market(n):
    rows = []
    used_countries = set()
    for i in range(1, n + 1):
        cc, cn = COUNTRIES[i % len(COUNTRIES)]
        if cc in used_countries:
            cc = f"{cc}{i}"
            cn = f"{cn} ({i})"
        used_countries.add(cc)
        region = REGIONS.get(cc[:2], "ROW")
        rows.append({
            "market_key": _hash(f"MKT-{cc}"),
            "market_code": cc,
            "market_name": cn,
            "country_code": cc[:2],
            "country_name": cn.split(" (")[0],
            "region_code": region,
            "region_name": region,
            "regulatory_authority": REGULATORY_AUTHORITIES.get(region),
            "primary_pharmacopoeia": PHARMACOPOEIAS.get(region, "USP"),
            "market_status": random.choice(["APPROVED", "PENDING", "FILED", "APPROVED", "APPROVED"]),
            "marketing_auth_number": f"MA-{cc[:2]}-{random.randint(10000,99999)}" if random.random() > 0.3 else None,
            "is_active": True,
            "load_timestamp": datetime.now().isoformat(),
        })
    return rows


def generate_dim_specification(n, products, materials, sites, markets):
    rows = []
    for i in range(1, n + 1):
        st_code, st_name = random.choice(SPEC_TYPES)
        ss_code, ss_name = random.choice(SPEC_STATUSES)
        sg_code, sg_name = random.choice(STAGES)
        spec_id = f"SPEC-{i:06d}"
        p = random.choice(products)
        m = random.choice(materials)
        s = random.choice(sites)
        mk = random.choice(markets)
        eff_start = _rand_date(2020, 2025)
        approval = eff_start - timedelta(days=random.randint(1, 30))
        rows.append({
            "spec_key": _hash(spec_id),
            "source_specification_id": spec_id,
            "spec_number": f"QC-SPEC-{2020 + (i % 7)}-{i:06d}",
            "spec_version": f"{random.randint(1,5)}.{random.randint(0,9)}",
            "spec_title": f"{p['inn_name'] or p['product_name']} {st_name} Specification",
            "spec_type_code": st_code,
            "spec_type_name": st_name,
            "product_key": p["product_key"],
            "material_key": m["material_key"],
            "site_key": s["site_key"],
            "market_key": mk["market_key"],
            "status_code": ss_code,
            "status_name": ss_name,
            "stage_code": sg_code,
            "stage_name": sg_name,
            "ctd_section": random.choice(["3.2.P.5.1", "3.2.S.4.1", "3.2.P.5.2", "3.2.S.4.2"]),
            "compendia_reference": random.choice(["USP", "EP", "JP", "BP", None]),
            "effective_start_date": str(eff_start),
            "effective_end_date": None,
            "effective_start_date_key": int(eff_start.strftime("%Y%m%d")),
            "effective_end_date_key": None,
            "approval_date": str(approval),
            "approval_date_key": int(approval.strftime("%Y%m%d")),
            "approver_name": f"Approver-{random.randint(1,50)}",
            "approved_by": f"user-{random.randint(1,50)}",
            "supersedes_spec_id": None,
            "supersedes_spec_key": None,
            "is_current": True,
            "valid_from": datetime.now().isoformat(),
            "valid_to": None,
            "load_timestamp": datetime.now().isoformat(),
        })
    return rows


def generate_dim_specification_item(specs, test_methods):
    rows = []
    item_count = 0
    test_names = ["Assay", "Dissolution", "Content Uniformity", "Impurity A", "Total Impurities",
                  "Hardness", "Friability", "Weight Variation", "Disintegration", "Description",
                  "Identification", "Moisture", "Residual Solvents", "Heavy Metals", "Microbial Limits",
                  "Uniformity of Dosage Units", "Particle Size", "pH", "Specific Rotation", "Loss on Drying"]
    for spec in specs:
        num_items = random.randint(4, 12)
        selected_tests = random.sample(test_names, min(num_items, len(test_names)))
        for seq, test in enumerate(selected_tests, 1):
            item_count += 1
            item_id = f"ITEM-{item_count:07d}"
            tm = random.choice(test_methods)
            cat_code, cat_name = random.choice(TEST_CATEGORIES)
            rows.append({
                "spec_item_key": _hash(item_id),
                "source_spec_item_id": item_id,
                "spec_key": spec["spec_key"],
                "test_method_key": tm["test_method_key"],
                "uom_key": None,  # will be resolved at load time
                "test_code": test[:4].upper(),
                "test_name": test,
                "analyte_code": test[:3].upper(),
                "test_category_code": cat_code,
                "test_category_name": cat_name,
                "test_subcategory": None,
                "criticality": random.choice(CRITICALITIES),
                "sequence_number": seq,
                "is_required": random.random() > 0.1,
                "is_compendial": random.random() > 0.3,
                "is_stability_indicating": random.random() > 0.5,
                "reporting_type": random.choice(["NUMERIC", "PASS_FAIL", "TEXT", "REPORT_ONLY"]),
                "result_precision": random.choice([0, 1, 2, 3]),
                "compendia_test_ref": tm.get("compendia_reference"),
                "stage_applicability": random.choice(["RELEASE", "STABILITY", "BOTH"]),
                "reporting_threshold": round(random.uniform(0.05, 0.2), 6) if cat_code == "IMP" else None,
                "identification_threshold": round(random.uniform(0.1, 0.5), 6) if cat_code == "IMP" else None,
                "qualification_threshold": round(random.uniform(0.2, 1.0), 6) if cat_code == "IMP" else None,
                "is_current": True,
                "valid_from": datetime.now().isoformat(),
                "valid_to": None,
                "load_timestamp": datetime.now().isoformat(),
            })
    return rows


def generate_dim_batch(n, products, sites):
    rows = []
    for i in range(1, n + 1):
        p = random.choice(products)
        s = random.choice(sites)
        mfg_date = _rand_date(2020, 2026)
        batch_number = f"B{mfg_date.year}-{i:06d}"
        rows.append({
            "batch_key": _hash(batch_number),
            "batch_number": batch_number,
            "batch_system_id": f"ERP-{random.randint(100000,999999)}",
            "product_key": p["product_key"],
            "site_key": s["site_key"],
            "batch_type": random.choice(BATCH_TYPES),
            "manufacturing_date": str(mfg_date),
            "expiry_date": str(mfg_date + timedelta(days=random.choice([730, 1095, 1460, 1825]))),
            "retest_date": str(mfg_date + timedelta(days=random.choice([365, 730, 1095]))) if random.random() > 0.7 else None,
            "batch_size": round(random.uniform(10, 500000), 4),
            "batch_size_unit": random.choice(["kg", "L", "units", "doses"]),
            "yield_pct": round(random.uniform(85, 102), 4),
            "batch_status": random.choice(BATCH_STATUSES),
            "disposition_date": str(mfg_date + timedelta(days=random.randint(1, 30))) if random.random() > 0.2 else None,
            "packaging_configuration": random.choice(CONTAINERS),
            "is_active": True,
            "load_timestamp": datetime.now().isoformat(),
        })
    return rows


def generate_dim_instrument(n):
    rows = []
    for i in range(1, n + 1):
        itype = random.choice(INSTRUMENT_TYPES)
        mfr = random.choice(INSTRUMENT_MANUFACTURERS)
        inst_id = f"INST-{i:05d}"
        rows.append({
            "instrument_key": _hash(inst_id),
            "instrument_id": inst_id,
            "instrument_name": f"{mfr} {itype} {random.randint(1000,9999)}",
            "instrument_type": itype,
            "serial_number": f"SN-{random.randint(100000,999999)}",
            "manufacturer": mfr,
            "qualification_status": random.choice(QUALIFICATION_STATUSES),
            "calibration_due_date": str(_rand_date(2025, 2027)),
            "location": f"Lab {random.randint(1,20)}, Room {random.randint(100,500)}",
            "is_active": True,
            "load_timestamp": datetime.now().isoformat(),
        })
    return rows


def generate_dim_laboratory(n, sites):
    rows = []
    for i in range(1, n + 1):
        s = random.choice(sites)
        lab_id = f"LAB-{i:04d}"
        rows.append({
            "laboratory_key": _hash(lab_id),
            "laboratory_id": lab_id,
            "laboratory_name": f"QC Lab {chr(65 + (i % 26))} - {s['site_name'][:20]}",
            "laboratory_type": random.choice(LAB_TYPES),
            "site_key": s["site_key"],
            "accreditation_status": random.choice(ACCREDITATION_STATUSES),
            "is_active": True,
            "load_timestamp": datetime.now().isoformat(),
        })
    return rows


def generate_fact_specification_limit(n, spec_items):
    rows = []
    # Pre-select items to create limits for
    items_sample = random.choices(spec_items, k=n)
    for i, item in enumerate(items_sample):
        lt = random.choice(LIMIT_TYPES)
        lower = round(random.uniform(80, 99), 6) if random.random() > 0.2 else None
        upper = round(random.uniform(101, 120), 6) if random.random() > 0.1 else None
        target = round(random.uniform(95, 105), 6) if lower or upper else None
        eff_date = _rand_date(2020, 2025)
        rows.append({
            "spec_limit_key": _hash(f"SL-{i+1:08d}"),
            "spec_key": item["spec_key"],
            "spec_item_key": item["spec_item_key"],
            # Keys match ROW_NUMBER() OVER (ORDER BY limit_type_code) in 00_populate_reference_data.sql
            "limit_type_key": {"AC": 1, "NOR": 5, "PAR": 6}.get(lt, 1),
            "uom_key": None,
            "effective_start_date_key": int(eff_date.strftime("%Y%m%d")),
            "effective_end_date_key": None,
            "lower_limit_value": lower,
            "upper_limit_value": upper,
            "target_value": target,
            "limit_range_width": round(upper - lower, 6) if lower and upper else None,
            "lower_limit_operator": "GTE" if lower else "NONE",
            "upper_limit_operator": "LTE" if upper else "NONE",
            "limit_text": None,
            "limit_description": f"{lower or 'NLT'} - {upper or 'NMT'}" if lower or upper else None,
            "limit_basis": random.choice(["AS_IS", "ANHYDROUS", "AS_LABELED", "DRIED_BASIS"]),
            "stage_code": random.choice(["RELEASE", "STABILITY", "BOTH"]),
            "stability_time_point": None,
            "stability_condition": None,
            "calculation_method": random.choice(["3_SIGMA", "CPK", "MANUAL", None]),
            "sample_size": random.randint(10, 100) if random.random() > 0.5 else None,
            "last_calculated_date_key": None,
            "is_in_filing": random.random() > 0.3,
            "regulatory_basis": random.choice(["ICH Q6A", "ICH Q3B", "USP", "EP", None]),
            "source_limit_id": f"LIM-{i+1:08d}",
            "is_current": True,
            "load_timestamp": datetime.now().isoformat(),
        })
    return rows


def generate_fact_analytical_result(n, batches, spec_items, instruments, laboratories):
    rows = []
    conditions = ["25C60RH", "30C65RH", "40C75RH", "5C"]
    timepoints = ["T0", "T3M", "T6M", "T12M", "T24M", "T36M"]
    statuses = ["PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "OOS", "OOT", "PENDING"]

    for i in range(1, n + 1):
        batch = random.choice(batches)
        item = random.choice(spec_items)
        inst = random.choice(instruments)
        lab = random.choice(laboratories)
        result_val = round(random.uniform(85, 115), 6) if random.random() > 0.1 else None
        status = random.choice(statuses)
        rep_lower = round(random.uniform(80, 98), 6) if random.random() > 0.2 else None
        rep_upper = round(random.uniform(102, 120), 6) if random.random() > 0.1 else None
        test_date = _rand_date(2020, 2026)

        rows.append({
            "analytical_result_key": _hash(f"RES-{i:08d}"),
            "batch_key": batch["batch_key"],
            "spec_key": item["spec_key"],
            "spec_item_key": item["spec_item_key"],
            "condition_key": random.randint(1, 6) if random.random() > 0.3 else None,
            "timepoint_key": random.randint(1, 9) if random.random() > 0.3 else None,
            "instrument_key": inst["instrument_key"],
            "laboratory_key": lab["laboratory_key"],
            "uom_key": None,
            "test_date_key": int(test_date.strftime("%Y%m%d")),
            "result_value": result_val,
            "result_text": None if result_val else "Conforms",
            "result_status_code": status,
            "percent_label_claim": round(result_val, 6) if result_val and random.random() > 0.5 else None,
            "reported_lower_limit": rep_lower,
            "reported_upper_limit": rep_upper,
            "reported_target": round(random.uniform(95, 105), 6) if random.random() > 0.3 else None,
            "is_oos": status == "OOS",
            "is_oot": status == "OOT",
            "sample_type": random.choice(["RELEASE", "STABILITY", "IPC", "INVESTIGATIONAL"]),
            "replicate_number": random.randint(1, 3),
            "analyst_name": f"Analyst-{random.randint(1,100)}",
            "reviewer_name": f"Reviewer-{random.randint(1,50)}",
            "lab_name": lab["laboratory_name"][:50],
            "report_id": f"RPT-{random.randint(10000,99999)}",
            "coa_number": f"CoA-{batch['batch_number']}-{random.randint(1,5)}" if random.random() > 0.3 else None,
            "stability_study_id": f"STAB-{random.randint(1,500)}" if random.random() > 0.4 else None,
            "source_result_id": f"SRC-RES-{i:08d}",
            "is_current": True,
            "load_timestamp": datetime.now().isoformat(),
        })
    return rows


def write_csv(rows, filepath):
    if not rows:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written {len(rows):>10,} rows -> {filepath}")


def generate_insert_sql(table_name, rows, filepath, batch_size=1000):
    """Generate SQL INSERT statements for Databricks."""
    if not rows:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    columns = list(rows[0].keys())
    col_str = ", ".join(columns)

    with open(filepath, 'w') as f:
        f.write(f"-- Synthetic data for {table_name}\n")
        f.write(f"-- Generated: {datetime.now().isoformat()}\n")
        f.write(f"-- Total rows: {len(rows)}\n\n")

        for batch_start in range(0, len(rows), batch_size):
            batch = rows[batch_start:batch_start + batch_size]
            f.write(f"INSERT INTO {table_name} ({col_str})\nVALUES\n")
            value_lines = []
            for row in batch:
                vals = []
                for c in columns:
                    v = row[c]
                    if v is None:
                        vals.append("NULL")
                    elif isinstance(v, bool):
                        vals.append("TRUE" if v else "FALSE")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    else:
                        escaped = str(v).replace("'", "''")
                        vals.append(f"'{escaped}'")
                value_lines.append(f"  ({', '.join(vals)})")
            f.write(",\n".join(value_lines))
            f.write(";\n\n")

    print(f"  Written {len(rows):>10,} rows -> {filepath}")


def main(output_dir="synthetic_data", fmt="csv"):
    print("=" * 60)
    print("Pharma Quality Synthetic Data Generator (PQ/CMC)")
    print("=" * 60)

    random.seed(42)  # reproducible

    print("\n[1/10] Generating dim_product...")
    products = generate_dim_product(NUM_PRODUCTS)

    print("[2/10] Generating dim_material...")
    materials = generate_dim_material(NUM_MATERIALS)

    print("[3/10] Generating dim_test_method...")
    test_methods = generate_dim_test_method(NUM_TEST_METHODS)

    print("[4/10] Generating dim_site...")
    sites = generate_dim_site(NUM_SITES)

    print("[5/10] Generating dim_market...")
    markets = generate_dim_market(NUM_MARKETS)

    print("[6/10] Generating dim_specification...")
    specifications = generate_dim_specification(NUM_SPECIFICATIONS, products, materials, sites, markets)

    print("[7/10] Generating dim_specification_item...")
    spec_items = generate_dim_specification_item(specifications, test_methods)

    print("[8/10] Generating dim_batch, dim_instrument, dim_laboratory...")
    batches = generate_dim_batch(NUM_BATCHES, products, sites)
    instruments = generate_dim_instrument(NUM_INSTRUMENTS)
    laboratories = generate_dim_laboratory(NUM_LABORATORIES, sites)

    print("[9/10] Generating fact_specification_limit...")
    fact_limits = generate_fact_specification_limit(NUM_FACT_SPEC_LIMITS, spec_items)

    print("[10/10] Generating fact_analytical_result...")
    fact_results = generate_fact_analytical_result(NUM_FACT_ANALYTICAL_RESULTS, batches, spec_items, instruments, laboratories)

    # Write outputs
    print(f"\nWriting {fmt.upper()} files to {output_dir}/...")
    writer = write_csv if fmt == "csv" else lambda rows, fp: generate_insert_sql(fp.split("/")[-1].replace(".sql", ""), rows, fp)

    ext = ".csv" if fmt == "csv" else ".sql"
    write_csv(products, os.path.join(output_dir, f"dim_product{ext}"))
    write_csv(materials, os.path.join(output_dir, f"dim_material{ext}"))
    write_csv(test_methods, os.path.join(output_dir, f"dim_test_method{ext}"))
    write_csv(sites, os.path.join(output_dir, f"dim_site{ext}"))
    write_csv(markets, os.path.join(output_dir, f"dim_market{ext}"))
    write_csv(specifications, os.path.join(output_dir, f"dim_specification{ext}"))
    write_csv(spec_items, os.path.join(output_dir, f"dim_specification_item{ext}"))
    write_csv(batches, os.path.join(output_dir, f"dim_batch{ext}"))
    write_csv(instruments, os.path.join(output_dir, f"dim_instrument{ext}"))
    write_csv(laboratories, os.path.join(output_dir, f"dim_laboratory{ext}"))
    write_csv(fact_limits, os.path.join(output_dir, f"fact_specification_limit{ext}"))
    write_csv(fact_results, os.path.join(output_dir, f"fact_analytical_result{ext}"))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    totals = {
        "dim_product": len(products),
        "dim_material": len(materials),
        "dim_test_method": len(test_methods),
        "dim_site": len(sites),
        "dim_market": len(markets),
        "dim_specification": len(specifications),
        "dim_specification_item": len(spec_items),
        "dim_batch": len(batches),
        "dim_instrument": len(instruments),
        "dim_laboratory": len(laboratories),
        "fact_specification_limit": len(fact_limits),
        "fact_analytical_result": len(fact_results),
    }
    for table, count in totals.items():
        marker = " (FACT)" if "fact_" in table else " (DIM)"
        print(f"  {table:<35} {count:>10,} rows{marker}")
    total = sum(totals.values())
    print(f"  {'TOTAL':<35} {total:>10,} rows")
    print(f"\nAll dimensions have 10K+ rows: {all(v >= 30 for k, v in totals.items() if 'dim_' in k)}")
    print(f"All facts have 100K+ rows: {all(v >= 100000 for k, v in totals.items() if 'fact_' in k)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic pharma quality data")
    parser.add_argument("--output-dir", default="synthetic_data", help="Output directory for CSV files")
    parser.add_argument("--format", choices=["csv", "sql"], default="csv", help="Output format")
    args = parser.parse_args()
    main(output_dir=args.output_dir, fmt=args.format)
