"""Manual data loading and DAC view creation for NIST ISODB.

This module handles:
- Loading manually curated CSV data into the database
- Creating manual data tables with proper schemas
- Creating the unified DAC adsorbent discovery view
- Status reporting for manual data tables
"""

import csv
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from src import database


# =============================================================================
# Configuration
# =============================================================================

MANUAL_DATA_DIR = Path(__file__).parent.parent / "manual_data" / "data"

MANUAL_TABLES = {
    "manual_dac_screening_score": {
        "csv_file": "dac_screening_score.csv",
        "id_column": "material_id",
    },
    "manual_adsorbent_families": {
        "csv_file": "adsorbent_families.csv",
        "id_column": "Id",  # Note: CSV uses 'Id', we'll map to 'id'
    },
    "manual_family_categorization": {
        "csv_file": "family_categorization.csv",
        "id_column": None,  # Uses auto-increment
    },
    "manual_dac_screening_detailed": {
        "csv_file": "dac_screening_detailed.csv",
        "id_column": "material_id",
    },
}


# =============================================================================
# Table Creation
# =============================================================================

def create_manual_tables(db_path: Path = database.DEFAULT_DB_PATH) -> None:
    """Create all manual data tables in the database."""
    conn = database.get_connection(db_path)
    cursor = conn.cursor()

    # 1. DAC Screening Score Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS manual_dac_screening_score (
            material_id TEXT PRIMARY KEY,
            material_name TEXT NOT NULL,
            DAC_potential_score REAL,
            quick_rationale TEXT,
            source_1 TEXT,
            source_2 TEXT,
            source_3 TEXT,
            local_updated TEXT
        )
    """)

    # 2. Adsorbent Families Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS manual_adsorbent_families (
            id INTEGER PRIMARY KEY,
            family_name TEXT NOT NULL UNIQUE,
            inclusion TEXT,
            core_capture_mode TEXT,
            local_updated TEXT
        )
    """)

    # 3. Family Categorization Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS manual_family_categorization (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            material_id TEXT NOT NULL,
            material_name TEXT NOT NULL,
            family_id INTEGER,
            family_name TEXT,
            recat_basis TEXT,
            family TEXT,
            subtype TEXT,
            confidence TEXT,
            notes TEXT,
            local_updated TEXT
        )
    """)

    # 4. DAC Screening Detailed Table (50+ columns)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS manual_dac_screening_detailed (
            material_id TEXT PRIMARY KEY,
            material_name TEXT,
            material_name_notes TEXT,
            Active_site_type_capture_mechanism TEXT,
            Active_site_type_capture_mechanism_notes TEXT,
            Form_factor_tested TEXT,
            Form_factor_tested_notes TEXT,
            Data_source_ref TEXT,
            Data_source_ref_notes TEXT,
            real_air_validation_level TEXT,
            real_air_validation_criteria TEXT,
            qCO2_400ppm_25C_RH50_mmol_g REAL,
            qCO2_400ppm_25C_RH50_mmol_g_notes TEXT,
            CO2_working_capacity_cycle_mmol_g REAL,
            CO2_working_capacity_cycle_mmol_g_notes TEXT,
            Full_cycle_time_min REAL,
            Full_cycle_time_min_notes TEXT,
            Sorbent_productivity_grav_kgCO2_per_kg_per_day REAL,
            Sorbent_productivity_grav_kgCO2_per_kg_per_day_notes TEXT,
            Sorbent_productivity_grav_kgCO2_per_kg_per_day_type TEXT,
            t90_adsorption_400ppm_RH50_min REAL,
            t90_adsorption_400ppm_RH50_min_notes TEXT,
            t90_desorption_regen_min REAL,
            t90_desorption_regen_min_notes TEXT,
            Humidity_penalty_RH70_pct REAL,
            Humidity_penalty_RH70_pct_notes TEXT,
            Temperature_penalty_0C_pct REAL,
            Temperature_penalty_0C_pct_notes TEXT,
            Pre_drying_required TEXT,
            Required_RH_threshold REAL,
            Lowest_regen_energy_total_scoped_MJ_per_kgCO2 REAL,
            Lowest_regen_energy_total_scoped_MJ_per_kgCO2_notes TEXT,
            Regen_temperature_C REAL,
            Regen_temperature_C_notes TEXT,
            Regen_mode TEXT,
            Regen_mode_notes TEXT,
            Air_side_pressure_drop_ref_Pa_per_m REAL,
            Air_side_pressure_drop_ref_Pa_per_m_notes TEXT,
            Recommended_face_velocity_m_per_s REAL,
            Recommended_face_velocity_m_per_s_notes TEXT,
            Estimated_sorbent_cost_usd_per_kg REAL,
            Estimated_sorbent_cost_usd_per_kg_notes TEXT,
            manufacturing_maturity TEXT,
            manufacturing_maturity_notes TEXT,
            manufacturing_complexity REAL,
            manufacturing_complexity_notes TEXT,
            supply_chain_risk_LMH TEXT,
            IP_FTO_status TEXT,
            Metric_confidence TEXT,
            Metric_confidence_notes TEXT,
            local_updated TEXT
        )
    """)

    # Create indexes for efficient querying
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_manual_dac_score_score
        ON manual_dac_screening_score(DAC_potential_score DESC)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_manual_family_cat_material
        ON manual_family_categorization(material_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_manual_family_cat_family
        ON manual_family_categorization(family_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_manual_dac_detailed_capacity
        ON manual_dac_screening_detailed(qCO2_400ppm_25C_RH50_mmol_g DESC)
    """)

    conn.commit()
    conn.close()

    print("Manual data tables created successfully")


# =============================================================================
# Data Loading
# =============================================================================

def _normalize_value(value: str, numeric: bool = False) -> Optional[str]:
    """Normalize a CSV value, handling empty strings and numeric conversion."""
    if value is None or value.strip() == "":
        return None
    if numeric:
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    return value.strip()


def load_dac_screening_score(
    db_path: Path = database.DEFAULT_DB_PATH,
    data_dir: Path = MANUAL_DATA_DIR
) -> int:
    """Load DAC screening score data from CSV."""
    conn = database.get_connection(db_path)
    cursor = conn.cursor()
    timestamp = datetime.utcnow().isoformat()

    csv_path = data_dir / "dac_screening_score.csv"
    records_loaded = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute("""
                INSERT OR REPLACE INTO manual_dac_screening_score
                (material_id, material_name, DAC_potential_score, quick_rationale,
                 source_1, source_2, source_3, local_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                _normalize_value(row["material_id"]),
                _normalize_value(row["material_name"]),
                _normalize_value(row["DAC_potential_score"], numeric=True),
                _normalize_value(row["quick_rationale"]),
                _normalize_value(row["source_1"]),
                _normalize_value(row["source_2"]),
                _normalize_value(row["source_3"]),
                timestamp,
            ))
            records_loaded += 1

    conn.commit()
    conn.close()
    return records_loaded


def load_adsorbent_families(
    db_path: Path = database.DEFAULT_DB_PATH,
    data_dir: Path = MANUAL_DATA_DIR
) -> int:
    """Load adsorbent families data from CSV."""
    conn = database.get_connection(db_path)
    cursor = conn.cursor()
    timestamp = datetime.utcnow().isoformat()

    csv_path = data_dir / "adsorbent_families.csv"
    records_loaded = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute("""
                INSERT OR REPLACE INTO manual_adsorbent_families
                (id, family_name, inclusion, core_capture_mode, local_updated)
                VALUES (?, ?, ?, ?, ?)
            """, (
                int(row["Id"]),  # CSV uses 'Id' with capital I
                _normalize_value(row["family_name"]),
                _normalize_value(row["inclusion"]),
                _normalize_value(row["core_capture_mode"]),
                timestamp,
            ))
            records_loaded += 1

    conn.commit()
    conn.close()
    return records_loaded


def load_family_categorization(
    db_path: Path = database.DEFAULT_DB_PATH,
    data_dir: Path = MANUAL_DATA_DIR
) -> int:
    """Load family categorization data from CSV."""
    conn = database.get_connection(db_path)
    cursor = conn.cursor()
    timestamp = datetime.utcnow().isoformat()

    # Clear existing data (since we use auto-increment)
    cursor.execute("DELETE FROM manual_family_categorization")

    csv_path = data_dir / "family_categorization.csv"
    records_loaded = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute("""
                INSERT INTO manual_family_categorization
                (material_id, material_name, family_id, family_name, recat_basis,
                 family, subtype, confidence, notes, local_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                _normalize_value(row["material_id"]),
                _normalize_value(row["material_name"]),
                _normalize_value(row["family_id"], numeric=True),
                _normalize_value(row["family_name"]),
                _normalize_value(row["recat_basis"]),
                _normalize_value(row["family"]),
                _normalize_value(row["subtype"]),
                _normalize_value(row["confidence"]),
                _normalize_value(row["notes"]),
                timestamp,
            ))
            records_loaded += 1

    conn.commit()
    conn.close()
    return records_loaded


def load_dac_screening_detailed(
    db_path: Path = database.DEFAULT_DB_PATH,
    data_dir: Path = MANUAL_DATA_DIR
) -> int:
    """Load detailed DAC screening data from CSV."""
    conn = database.get_connection(db_path)
    cursor = conn.cursor()
    timestamp = datetime.utcnow().isoformat()

    # Numeric columns that need conversion
    numeric_columns = {
        "qCO2_400ppm_25C_RH50_mmol_g",
        "CO2_working_capacity_cycle_mmol_g",
        "Full_cycle_time_min",
        "Sorbent_productivity_grav_kgCO2_per_kg_per_day",
        "t90_adsorption_400ppm_RH50_min",
        "t90_desorption_regen_min",
        "Humidity_penalty_RH70_pct",
        "Temperature_penalty_0C_pct",
        "Required_RH_threshold",
        "Lowest_regen_energy_total_scoped_MJ_per_kgCO2",
        "Regen_temperature_C",
        "Air_side_pressure_drop_ref_Pa_per_m",
        "Recommended_face_velocity_m_per_s",
        "Estimated_sorbent_cost_usd_per_kg",
        "manufacturing_complexity",
    }

    csv_path = data_dir / "dac_screening_detailed.csv"
    records_loaded = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames

        for row in reader:
            values = []
            for col in columns:
                is_numeric = col in numeric_columns
                values.append(_normalize_value(row[col], numeric=is_numeric))
            values.append(timestamp)  # local_updated

            placeholders = ", ".join(["?"] * len(values))
            col_names = ", ".join(columns) + ", local_updated"

            cursor.execute(f"""
                INSERT OR REPLACE INTO manual_dac_screening_detailed
                ({col_names})
                VALUES ({placeholders})
            """, values)
            records_loaded += 1

    conn.commit()
    conn.close()
    return records_loaded


def load_all_manual_data(
    db_path: Path = database.DEFAULT_DB_PATH,
    data_dir: Path = MANUAL_DATA_DIR,
    dry_run: bool = False
) -> dict:
    """Load all manual data from CSV files into the database.

    Args:
        db_path: Path to the SQLite database
        data_dir: Path to the directory containing CSV files
        dry_run: If True, only report what would be loaded without making changes

    Returns:
        Dictionary with loading statistics
    """
    result = {
        "tables_loaded": 0,
        "total_records": 0,
        "details": {},
    }

    if dry_run:
        print("\n[DRY RUN] Would load the following data:")
        for table_name, config in MANUAL_TABLES.items():
            csv_path = data_dir / config["csv_file"]
            if csv_path.exists():
                with open(csv_path, "r", encoding="utf-8") as f:
                    row_count = sum(1 for _ in f) - 1  # Subtract header
                print(f"  {table_name}: {row_count} records from {config['csv_file']}")
                result["details"][table_name] = row_count
                result["total_records"] += row_count
                result["tables_loaded"] += 1
            else:
                print(f"  {table_name}: CSV file not found: {csv_path}")
        return result

    # Create tables first
    create_manual_tables(db_path)

    # Load each table
    loaders = {
        "manual_dac_screening_score": load_dac_screening_score,
        "manual_adsorbent_families": load_adsorbent_families,
        "manual_family_categorization": load_family_categorization,
        "manual_dac_screening_detailed": load_dac_screening_detailed,
    }

    for table_name, loader_func in loaders.items():
        try:
            count = loader_func(db_path, data_dir)
            result["details"][table_name] = count
            result["total_records"] += count
            result["tables_loaded"] += 1
            print(f"  Loaded {count} records into {table_name}")
        except Exception as e:
            print(f"  Error loading {table_name}: {e}")
            result["details"][table_name] = f"Error: {e}"

    return result


# =============================================================================
# DAC Discovery View
# =============================================================================

def create_dac_views(db_path: Path = database.DEFAULT_DB_PATH) -> None:
    """Create all DAC-related analysis views."""
    conn = database.get_connection(db_path)
    cursor = conn.cursor()

    print("Creating DAC discovery views...")

    # Drop existing views
    cursor.execute("DROP VIEW IF EXISTS dac_adsorbent_discovery")
    cursor.execute("DROP VIEW IF EXISTS materials_by_family")
    cursor.execute("DROP VIEW IF EXISTS top_dac_candidates")  # Legacy, now removed
    cursor.execute("DROP VIEW IF EXISTS max_adsorption_dac_conditions")

    # Max Adsorption under DAC Conditions View
    # DAC conditions: CO2, 290-300K (~17-27°C), 80000-105000 Pa (~0.8-1.05 bar)
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS max_adsorption_dac_conditions AS
        SELECT
            material_id,
            material_name,
            MAX(adsorption_mol_per_kg) AS max_adsorption_mol_per_kg
        FROM materials_comparison
        WHERE gas_name = 'Carbon Dioxide'
            AND temperature BETWEEN 290 AND 300
            AND pressure_pa BETWEEN 80000 AND 105000
            AND adsorption_mol_per_kg >= 0
        GROUP BY material_id, material_name
        ORDER BY max_adsorption_mol_per_kg DESC
    """)

    # Main DAC Adsorbent Discovery View
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS dac_adsorbent_discovery AS
        SELECT
            -- NIST Material Data
            m.material_id,
            m.name AS material_name,
            m.formula,
            m.category AS nist_category,
            m.isotherm_count,

            -- Family Categorization
            fc.family_name,
            fc.subtype AS family_subtype,
            fc.confidence AS categorization_confidence,
            af.core_capture_mode,

            -- DAC Potential Score
            ds.DAC_potential_score,
            ds.quick_rationale AS score_rationale,

            -- Detailed DAC Screening Metrics (1 mmol/g = 1 mol/kg)
            dd.qCO2_400ppm_25C_RH50_mmol_g AS dac_capacity_mol_per_kg,
            dd.CO2_working_capacity_cycle_mmol_g AS working_capacity_mol_per_kg,
            dd.Regen_temperature_C AS regeneration_temp_C,
            dd.Lowest_regen_energy_total_scoped_MJ_per_kgCO2 AS regen_energy_MJ_per_kg,
            dd.Active_site_type_capture_mechanism AS capture_mechanism,
            dd.Form_factor_tested AS form_factor,
            dd.real_air_validation_level,
            dd.Metric_confidence,
            dd.Sorbent_productivity_grav_kgCO2_per_kg_per_day AS productivity_kg_per_kg_per_day,
            dd.Regen_mode AS regeneration_mode,

            -- NIST CO2 Performance under DAC conditions (290-300K, 0.8-1.05 bar)
            dac_nist.max_adsorption_mol_per_kg AS NIST_max_adsorption_mol_per_kg_dac,

            -- Data Availability Flags
            CASE WHEN ds.material_id IS NOT NULL THEN 1 ELSE 0 END AS has_dac_score,
            CASE WHEN dd.material_id IS NOT NULL THEN 1 ELSE 0 END AS has_detailed_screening,
            CASE WHEN fc.material_id IS NOT NULL THEN 1 ELSE 0 END AS has_family,
            CASE WHEN dac_nist.material_id IS NOT NULL THEN 1 ELSE 0 END AS has_nist_dac_data,

            -- Sources
            ds.source_1 AS dac_source_1,
            ds.source_2 AS dac_source_2,
            ds.source_3 AS dac_source_3

        FROM materials m
        LEFT JOIN manual_family_categorization fc ON m.material_id = fc.material_id
        LEFT JOIN manual_adsorbent_families af ON fc.family_id = af.id
        LEFT JOIN manual_dac_screening_score ds ON m.material_id = ds.material_id
        LEFT JOIN manual_dac_screening_detailed dd ON m.material_id = dd.material_id
        LEFT JOIN max_adsorption_dac_conditions dac_nist ON m.material_id = dac_nist.material_id

        ORDER BY COALESCE(ds.DAC_potential_score, 0) DESC
    """)

    # Materials by Family View
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS materials_by_family AS
        SELECT
            af.id AS family_id,
            af.family_name,
            af.core_capture_mode,
            COUNT(fc.material_id) AS material_count,
            AVG(ds.DAC_potential_score) AS avg_dac_score,
            MAX(ds.DAC_potential_score) AS max_dac_score,
            MIN(ds.DAC_potential_score) AS min_dac_score,
            GROUP_CONCAT(DISTINCT fc.subtype) AS subtypes
        FROM manual_adsorbent_families af
        LEFT JOIN manual_family_categorization fc ON af.id = fc.family_id
        LEFT JOIN manual_dac_screening_score ds ON fc.material_id = ds.material_id
        GROUP BY af.id
        ORDER BY avg_dac_score DESC
    """)

    conn.commit()
    conn.close()

    print("DAC views created successfully:")
    print("  - max_adsorption_dac_conditions (NIST CO2 at 290-300K, 0.8-1.05 bar)")
    print("  - dac_adsorbent_discovery (main unified view)")
    print("  - materials_by_family (family-level statistics)")


# =============================================================================
# Status Reporting
# =============================================================================

def get_manual_data_status(db_path: Path = database.DEFAULT_DB_PATH) -> dict:
    """Get status information for all manual data tables.

    Returns:
        Dictionary with table statistics
    """
    conn = database.get_connection(db_path)
    cursor = conn.cursor()

    status = {}

    for table_name in MANUAL_TABLES:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]

            cursor.execute(f"SELECT MAX(local_updated) FROM {table_name}")
            last_updated = cursor.fetchone()[0]

            # Count materials linked to NIST (for tables with material_id)
            if table_name != "manual_adsorbent_families":
                cursor.execute(f"""
                    SELECT COUNT(DISTINCT t.material_id)
                    FROM {table_name} t
                    INNER JOIN materials m ON t.material_id = m.material_id
                """)
                linked = cursor.fetchone()[0]
            else:
                linked = None

            status[table_name] = {
                "count": count,
                "last_updated": last_updated,
                "linked_to_nist": linked,
            }
        except sqlite3.OperationalError:
            # Table doesn't exist
            status[table_name] = {
                "count": 0,
                "last_updated": None,
                "linked_to_nist": None,
                "error": "Table not created",
            }

    # Add view statistics if they exist
    try:
        cursor.execute("SELECT COUNT(*) FROM dac_adsorbent_discovery WHERE has_dac_score = 1")
        materials_with_score = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dac_adsorbent_discovery WHERE has_detailed_screening = 1")
        materials_with_detailed = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dac_adsorbent_discovery WHERE has_nist_dac_data = 1")
        materials_with_nist_dac = cursor.fetchone()[0]

        status["views"] = {
            "dac_adsorbent_discovery": {
                "materials_with_score": materials_with_score,
                "materials_with_detailed": materials_with_detailed,
                "materials_with_nist_dac": materials_with_nist_dac,
            },
        }
    except sqlite3.OperationalError:
        status["views"] = {"error": "Views not created - run create-dac-views"}

    conn.close()
    return status
