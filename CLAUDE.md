# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# NIST ISODB Adsorbent Materials Scraper

## Project Overview

This project maintains a **searchable, normalized database** of adsorbent materials by periodically fetching data from the NIST/ARPA-E Database of Novel and Emerging Adsorbent Materials (ISODB).

### Features
- **Material Registry**: 9,300+ adsorbents (MOFs, zeolites, carbons, etc.)
- **Isotherm Database**: 39,800+ adsorption isotherms with standardized units
- **Gas Library**: 455 adsorbate molecules with InChIKey identifiers
- **Analysis Views**: Pre-computed material comparisons and performance rankings
- **Unit Normalization**: Automatic conversion to mol/kg (adsorption) and Pa (pressure)

**The scraper runs weekly to sync with NIST's updates. Views are refreshed after each sync.**

## NIST ISODB Data Source

- **Main Database**: https://adsorption.nist.gov/isodb/index.php
- **Materials Registry**: https://adsorption.nist.gov/matdb/index.php
- **API Documentation**: https://adsorption.nist.gov/isodb/index.php#apis
- **GitHub Mirror**: https://github.com/NIST-ISODB/isodb-library
- **User Guide PDF**: https://adsorption.nist.gov/isodb/content/media/userguide.pdf

### API Endpoints

The NIST ISODB serves data in **JSON**, **XML**, and **CSV** formats:

```
# List all adsorbent materials
https://adsorption.nist.gov/isodb/api/materials.json

# List all adsorbate gases
https://adsorption.nist.gov/isodb/api/gases.json

# List all isotherms
https://adsorption.nist.gov/isodb/api/isotherms.json

# Get specific isotherm by filename
https://adsorption.nist.gov/isodb/api/isotherm/<filename>.json

# List all bibliography entries
https://adsorption.nist.gov/isodb/api/biblio.json

# Materials Registry API
https://adsorption.nist.gov/matdb/api/materials.json
```

## Adsorbent Materials Data Schema

The materials table captures:

| Field | Description |
|-------|-------------|
| `material_id` | NIST unique identifier (hashKey) |
| `name` | Primary material name |
| `synonyms` | Alternative names (e.g., HKUST-1, CuBTC, MOF-199) |
| `formula` | Chemical formula if available |
| `category` | Material type (MOF, zeolite, carbon, etc.) |
| `isotherm_count` | Number of associated isotherms |
| `last_updated` | Timestamp of last NIST update |
| `local_updated` | Timestamp of last local sync |

## Data Analysis Layer

The project includes a normalization and analysis layer built on top of the raw NIST data:

### Normalized Views

#### `isotherm_data_points_normalized`
Standardized measurement data with unit conversions:
- **Pressure**: Converted to Pa (from bar, kPa, atm, etc.)
- **Adsorption**: Converted to mol/kg (from mmol/g, mg/g, etc.)
- **Filtering**: Pure component systems only (excludes gas mixtures)
- **Exclusions**: Area-based units (mg/m²) excluded due to missing BET surface area data

#### `materials_comparison`
Cross-material comparison by gas, temperature, and pressure:
```sql
-- Example: Find best N2 adsorbent at 77K, 1 bar
SELECT material_name, MAX(adsorption_mol_per_kg) as capacity
FROM materials_comparison
WHERE gas_name = 'Nitrogen'
  AND temperature BETWEEN 75 AND 79
  AND pressure_pa BETWEEN 95000 AND 105000
GROUP BY material_id
ORDER BY capacity DESC LIMIT 10;
```

#### `material_isotherms`
Material-centric view showing all isotherms and associated data for a specific adsorbent:
```sql
-- Example: View all CO2 isotherms for ZIF-8
SELECT * FROM material_isotherms
WHERE material_name LIKE '%ZIF-8%'
  AND gases LIKE '%Carbon dioxide%';
```

#### `gas_material_matrix` (Materialized Table)
Pre-computed performance matrix for rapid queries:
- Bucketed by temperature (10K intervals) and pressure (0.1 bar resolution)
- Aggregated statistics (avg, max, min adsorption) per material-gas pair
- Indexed for fast "best material" lookups
- Refresh after each sync: `python main.py refresh-matrix`

### Naming Convention

**Source tables** (never modified after scraping):
- `materials`, `isotherms`, `isotherm_data_points`, `gases`, `bibliography`

**Derived tables/views** (computed from source data):
- `*_normalized` - Unit-standardized views
- `*_comparison` - Analysis/filtering views
- `*_matrix` - Pre-aggregated materialized tables

### Unit Standardization

**Standard Units:**
- Pressure: **Pascal (Pa)** - SI standard
- Adsorption: **mol/kg** - intensive property for material comparison
- Temperature: **Kelvin (K)** - already standardized in source data

**Conversion Strategy:**
1. `unit_conversion_factors` table stores conversion multipliers
2. Normalized views apply conversions automatically via SQL
3. Original values and units preserved in `*_original` columns
4. Unknown/unsupported units result in NULL (logged for manual review)

**Supported Conversions:**
| From | To | Factor |
|------|-----|--------|
| bar | Pa | 1×10⁵ |
| kPa | Pa | 1×10³ |
| atm | Pa | 101,325 |
| mmol/g | mol/kg | 1.0 |
| mg/g | mol/kg | 1 / M (molar mass) |

**Unsupported** (excluded from normalized views):
- Area-based units (mg/m², mol/m²) - require BET surface area data
- Multi-component isotherms - require partial pressure analysis

### Query Examples

**1. Find materials with highest CO2 capacity at specific conditions:**
```sql
SELECT material_name, gas_name, adsorption_mol_per_kg, temperature, pressure_pa
FROM materials_comparison
WHERE gas_name = 'Carbon dioxide'
  AND temperature = 298  -- Room temperature
  AND pressure_pa BETWEEN 100000 AND 200000  -- 1-2 bar
ORDER BY adsorption_mol_per_kg DESC
LIMIT 20;
```

**2. Compare specific material across different gases:**
```sql
SELECT gas_name, temperature, AVG(adsorption_mol_per_kg) as avg_capacity
FROM materials_comparison
WHERE material_name LIKE '%HKUST-1%'
GROUP BY gas_name, temperature
ORDER BY gas_name, temperature;
```

**3. Temperature dependence of adsorption for a material-gas pair:**
```sql
SELECT temperature, pressure_pa, adsorption_mol_per_kg
FROM materials_comparison
WHERE material_name = 'Zeolite 13X'
  AND gas_name = 'Methane'
ORDER BY temperature, pressure_pa;
```

**4. List all materials tested with a specific gas:**
```sql
SELECT DISTINCT material_name, material_category, COUNT(DISTINCT isotherm_filename) as isotherm_count
FROM material_isotherms
WHERE gases LIKE '%Hydrogen%'
GROUP BY material_id
ORDER BY isotherm_count DESC;
```

### Data Quality and Limitations

**Known Issues:**
1. **Unit metadata was missing** from bulk API responses - requires individual isotherm fetching (sync-data-points)
2. **Incomplete data**: Only ~20 of 39,825 isotherms have been individually fetched (as of initial analysis)
3. **NULL adsorption values**: ~3% of data points have NULL total_adsorption (species_data only)
4. **Missing isotherm types**: 49% of isotherms lack isotherm_type classification (absolute vs excess)

**Validation Rules:**
- Pressure and adsorption must be non-negative
- Temperature range: -200K to 1,073K (validated, but wide range may indicate data errors)
- Unit standardization failures result in NULL with logging

**Mixture Handling:**
- Multi-component isotherms are excluded from `materials_comparison` view
- Composition data (mole fractions) available in source `species_data` JSON
- Future enhancement: Separate mixture analysis views

### CLI Commands

```bash
# Sync all source data from NIST
python main.py sync

# Fetch individual isotherm data points (long-running)
python main.py sync-data-points --batch-size 1000

# Check data fetch progress
python main.py data-status

# Create/refresh normalized views (after schema changes)
python main.py create-views

# Refresh materialized matrix (after new data sync)
python main.py refresh-matrix

# Check normalization status
python main.py normalization-status

# Database status and record counts
python main.py status
```

## Critical Constraints

### API Rate Limiting
- **Be respectful of NIST servers** - implement delays between requests (0.1 second delay = 10 requests/second max)
- The API is public but not intended for aggressive scraping
- Consider caching responses to minimize redundant calls

### Change Detection Strategy
1. Compare record counts between runs
2. Store checksums/hashes of material records
3. Track `last_modified` headers from API responses
4. Flag suspicious changes (e.g., large deletions) and require confirmation before applying
5. Maintain audit trail of all changes

### Data Integrity
- Never delete local records without explicit confirmation
- Create backups before syncing
- Support dry-run mode for testing changes

## External Tools

- **isodbtools**: NIST's official Python library for ISODB
  ```bash
  pip install git+https://github.com/NIST-ISODB/isodbtools.git#egg=isodbtools
  ```
- **Isotherm Digitizer**: http://digitizer.matscreen.com/digitizer

## Attribution

Data source: NIST/ARPA-E Database of Novel and Emerging Adsorbent Materials
Database maintained by: Office of Data Informatics, Material Measurement Laboratory (MML), NIST
Contact: https://adsorption.nist.gov
