"""NIST ISODB API client for fetching adsorbent materials."""

import json
import time
from typing import Optional, Callable

import requests

from src.utils import rate_limited_get, calculate_checksum

# NIST ISODB API endpoints
MATERIALS_API_URL = "https://adsorption.nist.gov/isodb/api/materials.json"
ISOTHERMS_API_URL = "https://adsorption.nist.gov/isodb/api/isotherms.json"
ISOTHERM_INDIVIDUAL_API_URL = "https://adsorption.nist.gov/isodb/api/isotherm/{}.json"
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

def extract_data_points(raw_isotherm: dict) -> list[dict]:
    """Extract data points from a raw isotherm record."""
    filename = raw_isotherm.get("filename", "")
    isotherm_data = raw_isotherm.get("isotherm_data", [])

    data_points = []
    for point in isotherm_data:
        data_point = {
            "isotherm_filename": filename,
            "pressure": point.get("pressure"),
            "total_adsorption": point.get("total_adsorption"),
            "species_data": json.dumps(point.get("species_data", [])),
        }
        data_points.append(data_point)

    return data_points


def fetch_isotherms(max_retries: int = 3, retry_delay: float = 5.0) -> tuple[list[dict], list[dict]]:
    """Fetch all isotherms from the NIST ISODB API.

    Returns:
        Tuple of (isotherm_metadata_list, data_points_list)
    """
    for attempt in range(max_retries):
        try:
            response = rate_limited_get(ISOTHERMS_API_URL)
            raw_isotherms = response.json()

            isotherms = []
            all_data_points = []

            for raw in raw_isotherms:
                isotherms.append(normalize_isotherm(raw))
                all_data_points.extend(extract_data_points(raw))

            return isotherms, all_data_points
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay)
            else:
                raise RuntimeError(f"Failed to fetch isotherms after {max_retries} attempts: {e}")

    # This should never be reached due to the raise above, but satisfies type checker
    raise RuntimeError("Failed to fetch isotherms")


def fetch_single_isotherm(filename: str, max_retries: int = 3, retry_delay: float = 5.0) -> Optional[dict]:
    """Fetch a single isotherm with all data points from NIST API.

    Args:
        filename: The isotherm filename (primary key)
        max_retries: Number of retry attempts
        retry_delay: Delay between retries in seconds

    Returns:
        Raw isotherm dict with isotherm_data array, or None if failed
    """
    url = ISOTHERM_INDIVIDUAL_API_URL.format(filename)

    for attempt in range(max_retries):
        try:
            response = rate_limited_get(url)
            raw_isotherm = response.json()
            return raw_isotherm
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print(f"Failed to fetch {filename}: {e}")
                return None

    return None


def batch_fetch_isotherm_data_points(
    filenames: list[str],
    progress_callback: Optional[callable] = None
) -> tuple[list[dict], list[dict], list[tuple[str, str]]]:
    """Fetch data points for a batch of isotherms.

    Args:
        filenames: List of isotherm filenames to fetch
        progress_callback: Optional callback function(current, total)

    Returns:
        Tuple of (all_data_points, isotherm_metadata, failed_isotherms)
        - all_data_points: List of data point dicts ready for database insertion
        - isotherm_metadata: List of dicts with filename, pressure_units, adsorption_units
        - failed_isotherms: List of (filename, error_message) tuples
    """
    all_data_points = []
    isotherm_metadata = []
    failed_isotherms = []

    total = len(filenames)

    for i, filename in enumerate(filenames):
        if progress_callback:
            progress_callback(i + 1, total)

        raw_isotherm = fetch_single_isotherm(filename)

        if raw_isotherm:
            data_points = extract_data_points(raw_isotherm)
            all_data_points.extend(data_points)

            # Extract unit metadata from raw isotherm
            metadata = {
                "filename": filename,
                "pressure_units": raw_isotherm.get("pressureUnits"),
                "adsorption_units": raw_isotherm.get("adsorptionUnits"),
            }
            isotherm_metadata.append(metadata)
        else:
            failed_isotherms.append((filename, "API request failed"))

    return all_data_points, isotherm_metadata, failed_isotherms


def normalize_isotherm(raw: dict) -> dict:
    """Normalize a raw isotherm record from the API - expanded to capture all fields."""
    # Extract adsorbates as JSON string of InChIKeys
    adsorbates_raw = raw.get("adsorbates", [])
    adsorbates = json.dumps([a.get("InChIKey", "") for a in adsorbates_raw])

    # Extract adsorbent info
    adsorbent = raw.get("adsorbent", {})
    adsorbent_id = adsorbent.get("hashkey")
    adsorbent_name = adsorbent.get("name")

    # Count data points
    isotherm_data = raw.get("isotherm_data", [])
    data_point_count = len(isotherm_data)

    normalized = {
        "filename": raw.get("filename", ""),
        "doi": raw.get("DOI"),
        "adsorbent_id": adsorbent_id,
        "adsorbent_name": adsorbent_name,
        "adsorbates": adsorbates,
        "category": raw.get("category"),
        "temperature": raw.get("temperature"),
        "tabular_data": raw.get("tabular_data", 0),
        "isotherm_type": raw.get("isotherm_type"),
        "article_source": raw.get("articleSource"),
        "date": raw.get("date"),
        "digitizer": raw.get("digitizer"),
        "adsorption_units": raw.get("adsorptionUnits"),
        "pressure_units": raw.get("pressureUnits"),
        "composition_type": raw.get("compositionType"),
        "concentration_units": raw.get("concentrationUnits"),
        "data_point_count": data_point_count,
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
