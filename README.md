# NIST ISODB Adsorbent Materials Database

A searchable, normalized database of adsorbent materials with automated data synchronization from the [NIST/ARPA-E Database of Novel and Emerging Adsorbent Materials (ISODB)](https://adsorption.nist.gov/isodb/).

## Features

- **🔄 Automated Sync**: Monthly synchronization with NIST ISODB (39,800+ isotherms)
- **📊 Material Registry**: 9,300+ adsorbent materials (MOFs, zeolites, carbons, etc.)
- **🧪 Gas Library**: 455 adsorbate molecules with InChIKey identifiers
- **📈 Analysis Views**: Pre-computed material comparisons and performance rankings
- **⚖️ Unit Normalization**: Automatic conversion to standardized units (Pa, mol/kg)
- **🔍 Advanced Filtering**: Query by gas, temperature, pressure, and material properties
- **📚 Bibliography**: 4,600+ research papers with metadata

## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd NISTScraper

# Install dependencies
pip install -r requirements.txt
```

### Basic Usage

```bash
# 1. Initial sync - fetch metadata from NIST
python main.py sync

# 2. Fetch detailed isotherm data points (long-running, ~66 hours)
python main.py sync-data-points --batch-size 1000

# 3. Create normalized analysis views
python main.py create-views

# 4. Check database status
python main.py status
```

## Database Schema

### Source Tables (Raw NIST Data)

- **`materials`** - Adsorbent materials (9,326 records)
- **`isotherms`** - Isotherm metadata (39,825 records)
- **`isotherm_data_points`** - Pressure-adsorption measurements
- **`gases`** - Adsorbate molecules (455 records)
- **`bibliography`** - Research papers (4,647 records)

### Analysis Views (Normalized Data)

- **`isotherm_data_points_normalized`** - Unit-standardized measurements
- **`materials_comparison`** - Cross-material performance comparison
- **`material_isotherms`** - Material-centric isotherm aggregation
- **`gas_material_matrix`** - Pre-computed performance matrix (materialized table)

### Manual Data Tables (DAC Screening)

- **`manual_dac_screening_score`** - DAC potential scores (0-100) for materials
- **`manual_dac_screening_detailed`** - Comprehensive DAC screening metrics (50+ columns)
- **`manual_adsorbent_families`** - Adsorbent family definitions (Zeolites, MOFs, etc.)
- **`manual_family_categorization`** - Material-to-family mapping

### DAC Discovery Views

- **`max_adsorption_dac_conditions`** - NIST CO2 adsorption at DAC conditions (290-300K, 0.8-1.05 bar)
- **`dac_adsorbent_discovery`** - Unified view combining NIST data with manual DAC screening
- **`materials_by_family`** - Family-level statistics and DAC score aggregates

## DAC Adsorbent Discovery

The `dac_adsorbent_discovery` view combines NIST isotherm data with manually curated DAC (Direct Air Capture) screening metrics for comprehensive material evaluation.

### Find Top DAC Candidates

```sql
SELECT material_name, family_name, DAC_potential_score,
       dac_capacity_mol_per_kg, capture_mechanism
FROM dac_adsorbent_discovery
WHERE DAC_potential_score >= 70
ORDER BY DAC_potential_score DESC
LIMIT 20;
```

### Compare DAC Performance by Family

```sql
SELECT family_name, core_capture_mode,
       material_count, avg_dac_score, max_dac_score
FROM materials_by_family
WHERE avg_dac_score IS NOT NULL
ORDER BY avg_dac_score DESC;
```

### Find Materials with Both NIST and DAC Data

```sql
SELECT material_name, DAC_potential_score,
       dac_capacity_mol_per_kg, NIST_max_adsorption_mol_per_kg_dac
FROM dac_adsorbent_discovery
WHERE has_dac_score = 1 AND has_nist_dac_data = 1
ORDER BY DAC_potential_score DESC;
```

## Example Queries

### Find Best CO₂ Adsorbent at Room Temperature

```sql
SELECT material_name, MAX(adsorption_mol_per_kg) as capacity
FROM materials_comparison
WHERE gas_name = 'Carbon dioxide'
  AND temperature BETWEEN 293 AND 303
  AND pressure_pa BETWEEN 90000 AND 110000  -- ~1 bar
GROUP BY material_id
ORDER BY capacity DESC
LIMIT 10;
```

### Compare Material Performance Across Gases

```sql
SELECT gas_name, temperature, AVG(adsorption_mol_per_kg) as avg_capacity
FROM materials_comparison
WHERE material_name LIKE '%HKUST-1%'
GROUP BY gas_name, temperature
ORDER BY gas_name, temperature;
```

### View All Isotherms for a Specific Material

```sql
SELECT * FROM material_isotherms
WHERE material_name LIKE '%ZIF-8%'
  AND gases LIKE '%Carbon dioxide%';
```

## Unit Normalization

All measurements are automatically converted to standard units:

| Measurement | Standard Unit | Supported Conversions |
|-------------|---------------|----------------------|
| **Pressure** | Pascal (Pa) | bar, kPa, atm, psi, mmHg, torr, MPa |
| **Adsorption** | mol/kg | mmol/g, mol/g, mol/kg |
| **Temperature** | Kelvin (K) | Already standardized |

**Original values are preserved** in `*_original` columns for reference.

## CLI Commands

### Data Synchronization

```bash
# Sync all metadata from NIST
python main.py sync

# Fetch individual isotherm data points
python main.py sync-data-points --batch-size 1000 --limit 1000

# Check data fetch progress
python main.py data-status
```

### Database Management

```bash
# View database status
python main.py status

# Create a backup
python main.py backup

# Restore from backup
python main.py restore <backup-file>

# List available backups
python main.py list-backups
```

### Analysis Views

```bash
# Create/refresh normalized views
python main.py create-views

# Refresh materialized performance matrix
python main.py refresh-matrix

# Check normalization status
python main.py normalization-status
```

### Manual Data & DAC Discovery

```bash
# Load manual data (DAC screening, family categorization)
python main.py load-manual-data

# Preview without applying changes
python main.py load-manual-data --dry-run

# Check manual data status
python main.py manual-data-status

# Create DAC discovery views
python main.py create-dac-views
```

## Project Structure

```
NISTScraper/
├── data/
│   └── materials.db          # SQLite database
├── manual_data/
│   ├── schemas/              # YAML schema definitions
│   │   ├── dac_screening_detailed.yml
│   │   ├── dac_screening_score.yml
│   │   ├── adsorbent_families.yml
│   │   └── family_categorization.yml
│   └── data/                 # CSV data files
│       ├── dac_screening_detailed.csv
│       ├── dac_screening_score.csv
│       ├── adsorbent_families.csv
│       └── family_categorization.csv
├── src/
│   ├── database.py           # Database operations
│   ├── scraper.py            # NIST API client
│   ├── sync.py               # Synchronization engine
│   ├── normalization.py      # Unit conversion & views
│   ├── manual_data.py        # Manual data loading & DAC views
│   └── utils.py              # Utility functions
├── main.py                   # CLI interface
├── README.md                 # This file
├── CLAUDE.md                 # AI assistant guidance
└── requirements.txt          # Python dependencies
```

## Data Sources

### NIST ISODB API Endpoints

- **Materials**: `https://adsorption.nist.gov/isodb/api/materials.json`
- **Isotherms**: `https://adsorption.nist.gov/isodb/api/isotherms.json`
- **Individual Isotherm**: `https://adsorption.nist.gov/isodb/api/isotherm/<filename>.json`
- **Gases**: `https://adsorption.nist.gov/isodb/api/gases.json`
- **Bibliography**: `https://adsorption.nist.gov/isodb/api/biblio.json`

### External Resources

- **Main Database**: https://adsorption.nist.gov/isodb/
- **Materials Registry**: https://adsorption.nist.gov/matdb/
- **API Documentation**: https://adsorption.nist.gov/isodb/index.php#apis
- **GitHub Mirror**: https://github.com/NIST-ISODB/isodb-library
- **User Guide (PDF)**: https://adsorption.nist.gov/isodb/content/media/userguide.pdf

## Data Quality & Limitations

### Known Issues

1. **Unit Metadata**: Requires individual isotherm fetching (bulk API lacks unit data)
2. **Mixed Units**: Various pressure and adsorption units require normalization
3. **Mixture Isotherms**: Multi-component systems excluded from comparison views
4. **Area-based Units**: mg/m² units excluded (require BET surface area data)

### Validation Rules

- Pressure and adsorption values must be non-negative
- Temperature range: -200K to 1,073K (validated)
- Unknown/unsupported units result in NULL with logging

## API Rate Limiting

**Be respectful of NIST servers:**
- Default rate limit: 0.1 second delay (10 requests/second max)
- Estimated full sync time: ~66 hours for 39,825 isotherms
- Automatic checkpointing every 1,000 isotherms
- Resume capability for interrupted syncs

## Change Detection Strategy

The scraper maintains data integrity through:

1. **Checksum Comparison**: MD5 hashes detect content changes
2. **Audit Trail**: All modifications logged to `audit_log` table
3. **Suspicious Change Detection**: Large deletions flagged (>10% threshold)
4. **Dry-run Mode**: Test changes before applying (`--dry-run` flag)
5. **Automatic Backups**: Created before each sync operation

## Development

### Running Tests

```bash
# Run with limited dataset for testing
python main.py sync-data-points --limit 100 --batch-size 100

# Dry-run to preview changes
python main.py sync --dry-run

# Verbose output for debugging
python main.py sync --verbose
```

### Database Access

```bash
# Direct SQLite access
sqlite3 data/materials.db

# Query examples
sqlite> SELECT COUNT(*) FROM materials;
sqlite> SELECT * FROM material_isotherms LIMIT 5;
```

## Contributing

Contributions are welcome! Please ensure:

1. Code follows existing patterns in `src/` modules
2. New CLI commands added to [main.py](main.py)
3. Database changes include migration logic
4. API calls respect rate limiting (0.1s delay)
5. Documentation updated in [CLAUDE.md](CLAUDE.md)

## Attribution

**Data Source**: NIST/ARPA-E Database of Novel and Emerging Adsorbent Materials
**Maintained by**: Office of Data Informatics, Material Measurement Laboratory (MML), NIST
**Contact**: https://adsorption.nist.gov

## License

This project is a data scraper and analysis tool. The underlying ISODB data is provided by NIST and subject to their terms of use.

## Acknowledgments

- NIST/ARPA-E for providing the ISODB public API
- Materials science community for data contribution
- [isodbtools](https://github.com/NIST-ISODB/isodbtools) - Official NIST Python library

---

**Last Updated**: January 2026
**Database Version**: 39,825 isotherms, 9,326 materials, 455 gases
