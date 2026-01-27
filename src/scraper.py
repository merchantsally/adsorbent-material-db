"""NIST ISODB API client for fetching adsorbent materials."""

import json
import time
from typing import Optional

import requests

from src.utils import rate_limited_get, calculate_checksum

# NIST ISODB API endpoints
MATERIALS_API_URL = "https://adsorption.nist.gov/isodb/api/materials.json"
ISOTHERMS_API_URL = "https://adsorption.nist.gov/isodb/api/isotherms.json"
GASES_API_URL = "https://adsorption.nist.gov/isodb/api/gases.json"
BIBLIO_API_URL = "https://adsorption.nist.gov/isodb/api/biblio.json"


def fetch_materials(max_retries: int = 3, retry_delay: float = 5.0) -> list[dict]:
    """Fetch all materials from the NIST ISODB API.

    Returns a list of normalized material records with checksums.
    """
    for attempt in range(max_retries):
        try:
            response = rate_limited_get(MATERIALS_API_URL)
            raw_materials = response.json()
            return [normalize_material(m) for m in raw_materials]
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay)
            else:
                raise RuntimeError(f"Failed to fetch materials after {max_retries} attempts: {e}")


def fetch_isotherms_count() -> dict[str, int]:
    """Fetch isotherm counts per material.

    Returns a mapping of material_id -> isotherm_count.
    This is optional enrichment data - failures are logged but don't stop the sync.
    """
    try:
        response = rate_limited_get(ISOTHERMS_API_URL)
        isotherms = response.json()

        # Count isotherms per material
        counts: dict[str, int] = {}
        for isotherm in isotherms:
            # Each isotherm has an "adsorbent" field with material info
            adsorbent = isotherm.get("adsorbent", {})
            material_id = adsorbent.get("hashkey") or adsorbent.get("name")
            if material_id:
                counts[material_id] = counts.get(material_id, 0) + 1

        return counts
    except requests.RequestException as e:
        print(f"Warning: Failed to fetch isotherm counts: {e}")
        return {}


def normalize_material(raw: dict) -> dict:
    """Normalize a raw material record from the API to our schema."""
    # The NIST API returns materials with these fields:
    # - hashkey: unique identifier
    # - name: primary name
    # - synonyms: list of alternative names
    # - formula: chemical formula (may be None)
    # - family: material category (MOF, zeolite, etc.)

    material_id = raw.get("hashkey") or raw.get("name", "").replace(" ", "_")

    # Handle synonyms - can be a list or None
    synonyms_raw = raw.get("synonyms", [])
    if isinstance(synonyms_raw, list):
        synonyms = "; ".join(synonyms_raw) if synonyms_raw else None
    else:
        synonyms = synonyms_raw

    normalized = {
        "material_id": material_id,
        "name": raw.get("name", ""),
        "synonyms": synonyms,
        "formula": raw.get("formula"),
        "category": raw.get("family"),
        "isotherm_count": 0,  # Will be enriched separately
        "last_updated": None,  # API doesn't provide this
    }

    # Calculate checksum for change detection
    normalized["checksum"] = calculate_checksum(normalized)

    return normalized


def enrich_with_isotherm_counts(materials: list[dict], counts: dict[str, int]) -> list[dict]:
    """Add isotherm counts to materials and recalculate checksums."""
    for material in materials:
        material_id = material["material_id"]
        material["isotherm_count"] = counts.get(material_id, 0)
        # Recalculate checksum with the new count
        material["checksum"] = calculate_checksum(material)
    return materials


# =============================================================================
# Isotherms fetching
# =============================================================================

def fetch_isotherms(max_retries: int = 3, retry_delay: float = 5.0) -> list[dict]:
    """Fetch all isotherms from the NIST ISODB API."""
    for attempt in range(max_retries):
        try:
            response = rate_limited_get(ISOTHERMS_API_URL)
            raw_isotherms = response.json()
            return [normalize_isotherm(i) for i in raw_isotherms]
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay)
            else:
                raise RuntimeError(f"Failed to fetch isotherms after {max_retries} attempts: {e}")


def normalize_isotherm(raw: dict) -> dict:
    """Normalize a raw isotherm record from the API."""
    # Extract adsorbates as JSON string of InChIKeys
    adsorbates_raw = raw.get("adsorbates", [])
    adsorbates = json.dumps([a.get("InChIKey", "") for a in adsorbates_raw])

    normalized = {
        "filename": raw.get("filename", ""),
        "doi": raw.get("DOI"),
        "adsorbent_id": raw.get("adsorbent", {}).get("hashkey"),
        "adsorbates": adsorbates,
        "category": raw.get("category"),
        "temperature": raw.get("temperature"),
        "tabular_data": raw.get("tabular_data", 0),
        "isotherm_type": raw.get("isotherm_type"),
    }

    normalized["checksum"] = calculate_checksum(normalized)
    return normalized


# =============================================================================
# Gases (adsorbates) fetching
# =============================================================================

def fetch_gases(max_retries: int = 3, retry_delay: float = 5.0) -> list[dict]:
    """Fetch all gases from the NIST ISODB API."""
    for attempt in range(max_retries):
        try:
            response = rate_limited_get(GASES_API_URL)
            raw_gases = response.json()
            return [normalize_gas(g) for g in raw_gases]
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay)
            else:
                raise RuntimeError(f"Failed to fetch gases after {max_retries} attempts: {e}")


def normalize_gas(raw: dict) -> dict:
    """Normalize a raw gas record from the API."""
    synonyms_raw = raw.get("synonyms", [])
    if isinstance(synonyms_raw, list):
        synonyms = "; ".join(synonyms_raw) if synonyms_raw else None
    else:
        synonyms = synonyms_raw

    normalized = {
        "inchikey": raw.get("InChIKey", ""),
        "name": raw.get("name", ""),
        "synonyms": synonyms,
    }

    normalized["checksum"] = calculate_checksum(normalized)
    return normalized


# =============================================================================
# Bibliography fetching
# =============================================================================

def fetch_bibliography(max_retries: int = 3, retry_delay: float = 5.0) -> list[dict]:
    """Fetch all bibliography entries from the NIST ISODB API."""
    for attempt in range(max_retries):
        try:
            response = rate_limited_get(BIBLIO_API_URL)
            raw_biblio = response.json()
            return [normalize_biblio(b) for b in raw_biblio]
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay)
            else:
                raise RuntimeError(f"Failed to fetch bibliography after {max_retries} attempts: {e}")


def normalize_biblio(raw: dict) -> dict:
    """Normalize a raw bibliography record from the API."""
    # Convert lists to JSON strings for storage
    authors = json.dumps(raw.get("authors", []))
    categories = json.dumps(raw.get("categories", []))
    adsorbents = json.dumps([a.get("hashkey", a.get("name", "")) for a in raw.get("adsorbents", [])])
    adsorbates = json.dumps([a.get("InChIKey", a.get("name", "")) for a in raw.get("adsorbates", [])])
    temperatures = json.dumps(raw.get("temperatures", []))
    pressures = json.dumps(raw.get("pressures", []))

    normalized = {
        "doi": raw.get("DOI", ""),
        "title": raw.get("title"),
        "journal": raw.get("journal"),
        "year": raw.get("year"),
        "authors": authors,
        "categories": categories,
        "adsorbents": adsorbents,
        "adsorbates": adsorbates,
        "temperatures": temperatures,
        "pressures": pressures,
    }

    normalized["checksum"] = calculate_checksum(normalized)
    return normalized
