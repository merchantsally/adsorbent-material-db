# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# NIST ISODB Adsorbent Materials Scraper

## Project Overview

This project maintains a local table of adsorbent materials by periodically fetching data from the NIST/ARPA-E Database of Novel and Emerging Adsorbent Materials (ISODB). **The scraper runs weekly to sync with NIST's updates.**

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

## Critical Constraints

### API Rate Limiting
- **Be respectful of NIST servers** - implement delays between requests (minimum 1 second recommended)
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
