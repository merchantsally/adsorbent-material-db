"""Unit normalization and analytical view creation for NIST ISODB data."""

import sqlite3
from pathlib import Path
from src import database


# =============================================================================
# Unit Conversion Data
# =============================================================================

UNIT_CONVERSIONS = {
    # Pressure conversions to Pa
    ("bar", "Pa", "pressure"): (1e5, 0, "bar to Pascal"),
    ("kPa", "Pa", "pressure"): (1e3, 0, "kilopascal to Pascal"),
    ("atm", "Pa", "pressure"): (101325, 0, "atmosphere to Pascal"),
    ("psi", "Pa", "pressure"): (6894.76, 0, "pounds per square inch to Pascal"),
    ("mmHg", "Pa", "pressure"): (133.322, 0, "millimeters of mercury to Pascal"),
    ("torr", "Pa", "pressure"): (133.322, 0, "torr to Pascal"),
    ("MPa", "Pa", "pressure"): (1e6, 0, "megapascal to Pascal"),

    # Adsorption conversions to mol/kg
    ("mmol/g", "mol/kg", "adsorption"): (1.0, 0, "millimoles per gram to moles per kilogram"),
    ("mol/kg", "mol/kg", "adsorption"): (1.0, 0, "moles per kilogram (identity)"),
    ("mol/g", "mol/kg", "adsorption"): (1000.0, 0, "moles per gram to moles per kilogram"),

    # Note: mg/g conversions require molar mass - handled separately in SQL CASE statements
}


def populate_unit_conversion_table(db_path: Path = database.DEFAULT_DB_PATH) -> None:
    """Create and populate the unit_conversion_factors table."""
    conn = database.get_connection(db_path)
    cursor = conn.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS unit_conversion_factors (
            from_unit TEXT NOT NULL,
            to_unit TEXT NOT NULL,
            conversion_type TEXT NOT NULL,
            factor REAL NOT NULL,
            offset REAL DEFAULT 0,
            description TEXT,
            PRIMARY KEY (from_unit, to_unit, conversion_type)
        )
    """)

    # Clear existing data
    cursor.execute("DELETE FROM unit_conversion_factors")

    # Insert conversion factors
    for (from_unit, to_unit, conv_type), (factor, offset, description) in UNIT_CONVERSIONS.items():
        cursor.execute("""
            INSERT INTO unit_conversion_factors
            (from_unit, to_unit, conversion_type, factor, offset, description)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (from_unit, to_unit, conv_type, factor, offset, description))

    conn.commit()
    conn.close()

    print(f"Populated unit_conversion_factors table with {len(UNIT_CONVERSIONS)} conversions")


# =============================================================================
# Normalized Views Creation
# =============================================================================

def create_normalized_views(db_path: Path = database.DEFAULT_DB_PATH) -> None:
    """Create all normalized analysis views.

    This creates:
    - isotherm_data_points_normalized
    - materials_comparison
    - material_isotherms
    """
    conn = database.get_connection(db_path)
    cursor = conn.cursor()

    print("Creating normalized views...")

    # Drop existing views
    cursor.execute("DROP VIEW IF EXISTS isotherm_data_points_normalized")
    cursor.execute("DROP VIEW IF EXISTS materials_comparison")
    cursor.execute("DROP VIEW IF EXISTS material_isotherms")

    # 1. isotherm_data_points_normalized
    cursor.execute("""
        CREATE VIEW isotherm_data_points_normalized AS
        SELECT
            dp.id,
            dp.isotherm_filename,
            i.adsorbent_id,
            i.adsorbent_name,
            i.adsorbates,
            i.temperature,
            i.category AS isotherm_category,
            i.isotherm_type,

            -- Normalized pressure (to Pa)
            CASE
                WHEN pconv.factor IS NOT NULL THEN dp.pressure * pconv.factor + COALESCE(pconv.offset, 0)
                ELSE NULL
            END AS pressure_pa,
            dp.pressure AS pressure_original,
            i.pressure_units AS pressure_units_original,

            -- Normalized adsorption (to mol/kg)
            CASE
                WHEN i.adsorption_units LIKE '%mmol/g%' THEN dp.total_adsorption * 1.0
                WHEN i.adsorption_units LIKE '%mol/kg%' THEN dp.total_adsorption
                WHEN i.adsorption_units LIKE '%mol/g%' THEN dp.total_adsorption * 1000.0
                WHEN aconv.factor IS NOT NULL THEN dp.total_adsorption * aconv.factor + COALESCE(aconv.offset, 0)
                ELSE NULL
            END AS adsorption_mol_per_kg,
            dp.total_adsorption AS adsorption_original,
            i.adsorption_units AS adsorption_units_original,

            -- Species data (for multi-component handling)
            dp.species_data,

            -- Metadata
            i.doi,
            i.article_source

        FROM isotherm_data_points dp
        JOIN isotherms i ON dp.isotherm_filename = i.filename
        LEFT JOIN unit_conversion_factors pconv
            ON pconv.from_unit = i.pressure_units
            AND pconv.to_unit = 'Pa'
            AND pconv.conversion_type = 'pressure'
        LEFT JOIN unit_conversion_factors aconv
            ON aconv.from_unit = i.adsorption_units
            AND aconv.to_unit = 'mol/kg'
            AND aconv.conversion_type = 'adsorption'

        WHERE i.pressure_units IS NOT NULL
            AND i.adsorption_units IS NOT NULL
            AND i.adsorption_units NOT LIKE '%/m%'  -- Exclude area-based units
            AND json_array_length(i.adsorbates) = 1  -- Pure components only (single adsorbate)
    """)

    print("  ✓ Created isotherm_data_points_normalized")

    # 2. materials_comparison
    cursor.execute("""
        CREATE VIEW materials_comparison AS
        SELECT
            m.material_id,
            m.name AS material_name,
            m.category AS material_category,
            m.formula AS material_formula,

            -- Gas information (iterate through all adsorbates in JSON array)
            g.name AS gas_name,
            g.inchikey AS gas_inchikey,

            -- Operating conditions
            n.temperature,
            n.pressure_pa,
            n.adsorption_mol_per_kg,

            -- Original values for reference
            n.pressure_original,
            n.pressure_units_original,
            n.adsorption_original,
            n.adsorption_units_original,

            -- Contextual metadata
            n.isotherm_type,
            n.isotherm_category,
            n.doi,
            n.isotherm_filename

        FROM isotherm_data_points_normalized n
        JOIN materials m ON n.adsorbent_id = m.material_id
        -- Iterate through all adsorbates in the JSON array
        CROSS JOIN json_each(n.adsorbates) AS adsorbate_array
        JOIN gases g ON g.inchikey = adsorbate_array.value

        WHERE n.adsorption_mol_per_kg IS NOT NULL
            AND n.pressure_pa IS NOT NULL

        ORDER BY n.adsorption_mol_per_kg DESC
    """)

    print("  ✓ Created materials_comparison")

    # 3. material_isotherms
    cursor.execute("""
        CREATE VIEW material_isotherms AS
        SELECT
            m.material_id,
            m.name AS material_name,
            m.formula,
            m.category AS material_category,
            m.isotherm_count,

            i.filename AS isotherm_filename,
            i.temperature,
            i.isotherm_type,
            i.category AS isotherm_category,
            i.pressure_units,
            i.adsorption_units,
            i.data_point_count,
            i.doi,
            i.article_source,

            -- Aggregated gas names
            (
                SELECT GROUP_CONCAT(g2.name, ', ')
                FROM json_each(i.adsorbates) ae2
                JOIN gases g2 ON g2.inchikey = json_extract(ae2.value, '$')
            ) AS gases,

            -- Data point statistics
            COUNT(dp.id) AS actual_data_points,
            MIN(dp.pressure) AS min_pressure,
            MAX(dp.pressure) AS max_pressure,
            MIN(dp.total_adsorption) AS min_adsorption,
            MAX(dp.total_adsorption) AS max_adsorption

        FROM materials m
        JOIN isotherms i ON m.material_id = i.adsorbent_id
        LEFT JOIN isotherm_data_points dp ON i.filename = dp.isotherm_filename

        GROUP BY m.material_id, i.filename
        ORDER BY m.name, i.temperature
    """)

    print("  ✓ Created material_isotherms")

    conn.commit()
    conn.close()

    print("✓ All normalized views created successfully")


# =============================================================================
# Gas-Material Performance Matrix (Materialized Table)
# =============================================================================

def create_gas_material_matrix(db_path: Path = database.DEFAULT_DB_PATH) -> None:
    """Create and populate the gas_material_matrix materialized table.

    This pre-computes aggregated performance data for fast queries.
    """
    conn = database.get_connection(db_path)
    cursor = conn.cursor()

    print("Creating gas_material_matrix...")

    # Drop existing table
    cursor.execute("DROP TABLE IF EXISTS gas_material_matrix")

    # Create materialized table with aggregated data
    cursor.execute("""
        CREATE TABLE gas_material_matrix AS
        SELECT
            m.material_id,
            m.name AS material_name,
            m.category AS material_category,
            g.inchikey AS gas_inchikey,
            g.name AS gas_name,

            -- Bucketed conditions for aggregation
            CAST(ROUND(n.temperature / 10) * 10 AS INTEGER) AS temperature_bucket,
            ROUND(n.pressure_pa / 100000.0, 1) AS pressure_bar_rounded,

            -- Aggregated performance metrics
            AVG(n.adsorption_mol_per_kg) AS avg_adsorption,
            MAX(n.adsorption_mol_per_kg) AS max_adsorption,
            MIN(n.adsorption_mol_per_kg) AS min_adsorption,
            COUNT(*) AS data_point_count,

            -- Sample metadata
            MIN(n.doi) AS sample_doi

        FROM isotherm_data_points_normalized n
        JOIN materials m ON n.adsorbent_id = m.material_id
        -- Iterate through all adsorbates in the JSON array
        CROSS JOIN json_each(n.adsorbates) AS adsorbate_array
        JOIN gases g ON g.inchikey = adsorbate_array.value

        WHERE n.adsorption_mol_per_kg IS NOT NULL
            AND n.pressure_pa IS NOT NULL

        GROUP BY
            m.material_id,
            g.inchikey,
            temperature_bucket,
            pressure_bar_rounded

        HAVING data_point_count >= 1
    """)

    # Create indexes for fast lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_gas_material_lookup
        ON gas_material_matrix(gas_name, temperature_bucket, pressure_bar_rounded, max_adsorption DESC)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_material_lookup
        ON gas_material_matrix(material_name, gas_name)
    """)

    # Get count
    cursor.execute("SELECT COUNT(*) FROM gas_material_matrix")
    count = cursor.fetchone()[0]

    conn.commit()
    conn.close()

    print(f"✓ Created gas_material_matrix with {count:,} entries")


def refresh_gas_material_matrix(db_path: Path = database.DEFAULT_DB_PATH) -> None:
    """Refresh the gas_material_matrix by recreating it."""
    print("Refreshing gas_material_matrix...")
    create_gas_material_matrix(db_path)
    print("✓ Matrix refresh complete")


# =============================================================================
# Main Setup Function
# =============================================================================

def setup_normalized_schema(db_path: Path = database.DEFAULT_DB_PATH) -> None:
    """Complete setup: create unit conversion table, views, and materialized matrix."""
    print("\n" + "="*60)
    print("SETTING UP NORMALIZED SCHEMA")
    print("="*60 + "\n")

    populate_unit_conversion_table(db_path)
    print()
    create_normalized_views(db_path)
    print()
    create_gas_material_matrix(db_path)

    print("\n" + "="*60)
    print("SETUP COMPLETE")
    print("="*60)


# =============================================================================
# Utility Functions
# =============================================================================

def drop_normalized_schema(db_path: Path = database.DEFAULT_DB_PATH) -> None:
    """Drop all normalized views and tables (for cleanup/reset)."""
    conn = database.get_connection(db_path)
    cursor = conn.cursor()

    print("Dropping normalized schema...")

    cursor.execute("DROP VIEW IF EXISTS isotherm_data_points_normalized")
    cursor.execute("DROP VIEW IF EXISTS materials_comparison")
    cursor.execute("DROP VIEW IF EXISTS material_isotherms")
    cursor.execute("DROP TABLE IF EXISTS gas_material_matrix")
    cursor.execute("DROP TABLE IF EXISTS unit_conversion_factors")

    conn.commit()
    conn.close()

    print("✓ Normalized schema dropped")


def get_normalization_stats(db_path: Path = database.DEFAULT_DB_PATH) -> dict:
    """Get statistics about normalized data coverage."""
    conn = database.get_connection(db_path)
    cursor = conn.cursor()

    stats = {}

    # Check if views exist
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='view' AND name IN (
            'isotherm_data_points_normalized',
            'materials_comparison',
            'material_isotherms'
        )
    """)
    stats["views_created"] = [row[0] for row in cursor.fetchall()]

    # Count normalized data points
    try:
        cursor.execute("SELECT COUNT(*) FROM isotherm_data_points_normalized")
        stats["normalized_data_points"] = cursor.fetchone()[0]
    except sqlite3.OperationalError:
        stats["normalized_data_points"] = 0

    # Count matrix entries
    try:
        cursor.execute("SELECT COUNT(*) FROM gas_material_matrix")
        stats["matrix_entries"] = cursor.fetchone()[0]
    except sqlite3.OperationalError:
        stats["matrix_entries"] = 0

    # Count isotherms with units
    cursor.execute("""
        SELECT COUNT(*) FROM isotherms
        WHERE pressure_units IS NOT NULL AND adsorption_units IS NOT NULL
    """)
    stats["isotherms_with_units"] = cursor.fetchone()[0]

    # Get distinct pressure and adsorption units
    cursor.execute("""
        SELECT DISTINCT pressure_units, COUNT(*)
        FROM isotherms
        WHERE pressure_units IS NOT NULL
        GROUP BY pressure_units
    """)
    stats["pressure_units"] = dict(cursor.fetchall())

    cursor.execute("""
        SELECT DISTINCT adsorption_units, COUNT(*)
        FROM isotherms
        WHERE adsorption_units IS NOT NULL
        GROUP BY adsorption_units
    """)
    stats["adsorption_units"] = dict(cursor.fetchall())

    conn.close()

    return stats
