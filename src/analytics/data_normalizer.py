from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Union

import normalization_mappings as nm

try:
    from src.analytics.report_generator import generate_pdf_report
except ImportError:
    from report_generator import generate_pdf_report


UNKNOWN_VALUE = "Desconocido"

# Input / output directories
INPUT_DIR = "data/output/data"
OUTPUT_DIR = "data/output"

NORMALIZED_DIR = os.path.join(OUTPUT_DIR, "normalized")
MULTITITLE_DIR = os.path.join(OUTPUT_DIR, "multititle")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(NORMALIZED_DIR, exist_ok=True)
os.makedirs(MULTITITLE_DIR, exist_ok=True)


def safe_get(obj: Any, key: str, default: Any = None) -> Any:
    """Safely get a key from a dict, returning ``default`` otherwise."""
    return obj.get(key, default) if isinstance(obj, dict) else default


def get_nested(obj: Any, path: str, default: Any = None) -> Any:
    """Get nested value using a slash-separated path (e.g. 'a/b/c')."""
    current = obj
    for key in path.split("/"):
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return default
    return current if current is not None else default


def ensure_list(value: Any) -> List[Any]:
    """Return a list for ``value`` (wrap non-list singletons)."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def extract_year(date_string: Optional[str]) -> Optional[str]:
    """Extract a 4-digit year (19xx or 20xx) from a string, else None."""
    if not date_string or not isinstance(date_string, str):
        return None
    match = re.search(r"\b(19|20)\d{2}\b", date_string)
    return match.group(0) if match else None


def normalize_dates(date_list: List[str]) -> Optional[List[Dict[str, str]]]:
    """Normalize date values to objects with `full` and `year`.

    Rules:
    - If value is invalid / empty -> {full: UNKNOWN_VALUE, year: UNKNOWN_VALUE}
    - If year cannot be extracted -> year = UNKNOWN_VALUE (not null)
    - Returns None when input list is empty
    """
    if not date_list:
        return None

    normalized: List[Dict[str, str]] = []
    for raw in date_list:
        if not raw or not isinstance(raw, str):
            normalized.append({"full": UNKNOWN_VALUE, "year": UNKNOWN_VALUE})
            continue

        full = raw.strip()
        if not full:
            normalized.append({"full": UNKNOWN_VALUE, "year": UNKNOWN_VALUE})
            continue

        year = extract_year(full) or UNKNOWN_VALUE
        normalized.append({"full": full, "year": year})

    return normalized if normalized else None


def clean_text_strict(text: str) -> str:
    """Remove non-letter chars, collapse spaces (keeps accents).

    Hyphens, underscores, dots and commas are treated as spaces to avoid
    merging initials or compound names.
    """
    if not text:
        return ""

    s = re.sub(r"[-_.,]", " ", text)
    s = re.sub(r"[^a-zA-ZáéíóúÁÉÍÓÚñÑüÜ ]", "", s)
    return re.sub(r"\s+", " ", s).strip()


def select_best_multilingual(value_list: Any, prefer_lang: str = "spa") -> Optional[str]:
    """From a list (or single) of values pick the best representation.

    Preference is based on the `@xml:lang` when present and on length
    (word count) as fallback.
    """
    if not value_list:
        return None

    values = ensure_list(value_list)
    if not values:
        return None

    candidates: List[Dict[str, Union[str, int]]] = []
    for item in values:
        text = ""
        lang = ""
        if isinstance(item, dict):
            text = item.get("#text", "")
            lang = item.get("@xml:lang", "").lower()
        elif isinstance(item, str):
            text = item
        if not text:
            continue
        candidates.append({"text": text, "lang": lang, "len": len(text.split())})

    if not candidates:
        return None

    preferred = [c for c in candidates if prefer_lang in c["lang"]]
    if preferred:
        preferred.sort(key=lambda x: x["len"], reverse=True)
        return preferred[0]["text"]

    candidates.sort(key=lambda x: x["len"], reverse=True)
    return candidates[0]["text"]


def normalize_title(title_value: Any) -> str:
    """Choose a title and discard purely technical ones (URLs, pure numbers)."""
    selected = select_best_multilingual(title_value)
    if not selected:
        return UNKNOWN_VALUE
    if isinstance(selected, str) and selected.startswith("http"):
        return UNKNOWN_VALUE
    if isinstance(selected, str) and selected.isdigit():
        return UNKNOWN_VALUE
    return selected.strip()


def normalize_creator(creator_value: Any) -> Optional[List[str]]:
    """Normalize creators: extract text, strict-clean and uppercase."""
    if not creator_value:
        return None

    def _proc(it: Any) -> str:
        if isinstance(it, dict):
            return it.get("#text", "")
        if isinstance(it, str):
            return it
        return ""

    out: List[str] = []
    for it in ensure_list(creator_value):
        name = _proc(it)
        if not name:
            continue
        clean = clean_text_strict(name)
        if clean:
            out.append(clean.upper())

    return out if out else None


def normalize_rights(rights_list: Any) -> Optional[List[str]]:
    """Normalize rights/access terms using mapping and heuristics."""
    if not rights_list:
        return None

    normalized: List[str] = []
    for item in ensure_list(rights_list):
        if not item or not isinstance(item, str):
            continue
        s = item.strip()
        low = s.lower()
        if s.startswith(("http://", "https://")):
            normalized.append(s)
            continue
        if s in nm.valid_rights.values():
            normalized.append(s)
            continue
        if "info:eu-repo/semantics/" in s:
            extracted = s.split("info:eu-repo/semantics/")[-1].strip()
            key = extracted.lower().replace("-", "").replace("_", "").replace(" ", "")
            found = False
            for k, v in nm.valid_rights.items():
                if k.replace(" ", "") == key:
                    normalized.append(v)
                    found = True
                    break
            if not found:
                if key in ["openaccess", "open"]:
                    normalized.append("openAccess")
                elif key in ["embargoedaccess", "embargoed"]:
                    normalized.append("embargoedAccess")
                elif key in ["restrictedaccess", "restricted"]:
                    normalized.append("restrictedAccess")
                elif key in ["closedaccess", "closed"]:
                    normalized.append("closedAccess")
            continue
        # fuzzy match
        comp = low.replace("-", "").replace("_", "").replace(" ", "")
        for k, v in nm.valid_rights.items():
            if k.replace(" ", "") in comp:
                normalized.append(v)
                break

    if not normalized:
        return None

    # dedupe preserving order
    unique = list(dict.fromkeys(normalized))
    priority = nm.access_terms_priority

    def _sort_key(x: str) -> int:
        if x in priority:
            return 0
        if x.startswith("http"):
            return 1
        return 2

    unique.sort(key=_sort_key)
    return unique


def normalize_description(desc_value: Any, repo_name: str = "") -> str:
    """Normalize description; special handling for XPLORA repos."""
    if "XPLORA" in repo_name:
        parts: List[str] = []
        for d in ensure_list(desc_value):
            if isinstance(d, dict):
                d = d.get("#text", "")
            if not isinstance(d, str):
                continue
            s = d.strip()
            if s.startswith("Sin ") or s.startswith("Sin_"):
                continue
            if s:
                parts.append(s)
        return " ".join(parts) if parts else UNKNOWN_VALUE

    sel = select_best_multilingual(desc_value)
    return sel if sel else UNKNOWN_VALUE


def normalize_type(type_value: Any) -> Union[str, List[str]]:
    """Normalize dc:type using explicit mapping in `normalization_mappings`."""
    if not type_value:
        return UNKNOWN_VALUE
    if isinstance(type_value, list):
        out: List[str] = []
        for t in type_value:
            res = normalize_type(t)
            if res and res != UNKNOWN_VALUE:
                if isinstance(res, list):
                    out.extend(res)
                else:
                    out.append(res)
        unique = list(dict.fromkeys(out))
        return unique[0] if unique else UNKNOWN_VALUE
    if not isinstance(type_value, str):
        return UNKNOWN_VALUE
    s = type_value.strip()
    if "info:eu-repo/semantics/" in s:
        s = s.replace("info:eu-repo/semantics/", "")
    if s in nm.type_mapping:
        return nm.type_mapping[s]
    if s in nm.type_mapping.values():
        return s
    return UNKNOWN_VALUE


def normalize_format(format_list: Any) -> Optional[List[str]]:
    """Normalize dc:format values to a set of known format keys."""
    if not format_list:
        return None
    out: List[str] = []
    for f in ensure_list(format_list):
        if not f or not isinstance(f, str):
            continue
        s = f.strip().lower()
        if s in nm.format_mapping:
            key = nm.format_mapping[s]
            if key in nm.valid_formats:
                out.append(key)
                continue
        if s in nm.valid_formats:
            out.append(s)
            continue
        for valid in nm.valid_formats:
            if valid == "unknown":
                continue
            if valid in s:
                out.append(valid)
                break
    return list(dict.fromkeys(out)) if out else None


def normalize_identifier(id_list: Any) -> Optional[List[str]]:
    """Keep only https identifiers from dc:identifier values.

    Returns a list of unique https URLs or None when none found.
    """
    if not id_list:
        return None
    out: List[str] = []
    for v in ensure_list(id_list):
        if not v or not isinstance(v, str):
            continue
        s = v.strip()
        if s.lower().startswith("https://"):
            out.append(s)
    return list(dict.fromkeys(out)) if out else None


def normalize_subject(subject_list: Any) -> Optional[List[str]]:
    """Normalize subjects using CTI mappings and whitelist.

    Returns `None` when nothing matches the allowed subjects.
    """
    """
    Improved subject normalization:
    - If DC subject contains CTI classification codes, map them to labels.
    - Otherwise keep the literal subject (uppercased, cleaned).
    - Preserve multiple values and remove duplicates while keeping order.
    """
    if not subject_list:
        return None

    out: List[str] = []
    seen = set()

    for s in ensure_list(subject_list):
        if not s or not isinstance(s, str):
            continue
        s_clean = s.strip()

        # Skip URLs or technical identifiers
        if s_clean.lower().startswith("http"):
            continue

        # CTI classification mapping
        if "info:eu-repo/classification/cti/" in s_clean:
            m = re.search(r"info:eu-repo/classification/cti/(\d+)", s_clean)
            if m:
                code = m.group(1)
                label = None
                if len(code) == 4 and code in nm.cti_discipline_mapping:
                    label = nm.cti_discipline_mapping[code]
                elif len(code) == 2 and code in nm.cti_field_mapping:
                    label = nm.cti_field_mapping[code]
                elif len(code) == 1 and code in nm.cti_area_mapping:
                    label = nm.cti_area_mapping[code]
                if label:
                    v = clean_text_strict(label).upper()
                    if v and v not in seen:
                        out.append(v)
                        seen.add(v)
                    continue

        # Librunam or other classification: take the last path segment
        if "info:eu-repo/classification/" in s_clean:
            seg = s_clean.split("/")[-1].strip()
            if seg:
                v = clean_text_strict(seg).upper()
                if v and v not in seen:
                    out.append(v)
                    seen.add(v)
                continue

        # Generic subject: keep cleaned uppercased value
        v = clean_text_strict(s_clean).upper()
        if v and v not in seen:
            out.append(v)
            seen.add(v)

    return out if out else None


def normalize_language(lang_input: Any) -> Optional[Union[str, List[str]]]:
    """Normalize language tags / names to ISO-639-3 codes where possible."""
    if not lang_input:
        return None
    def _one(v: Any) -> Optional[str]:
        if not v or not isinstance(v, str):
            return None
        c = v.strip().lower()
        if c in nm.valid_iso_639_3:
            return c
        if len(c) == 2 and c in nm.iso_639_1_to_3:
            return nm.iso_639_1_to_3[c]
        return None
    out: List[str] = []
    for v in ensure_list(lang_input):
        r = _one(v)
        if r:
            out.append(r)
    if not out:
        return None
    unique = list(dict.fromkeys(out))
    return unique if len(unique) > 1 else unique[0]


def normalize_record(record: Any, repository_name: str) -> Dict[str, Any]:
    """Normalize a single OAI-PMH record into a normalized dict."""
    header = safe_get(record, "header", {})
    metadata = safe_get(record, "metadata", {})
    dc = safe_get(metadata, "oai_dc:dc", {})

    return {
        "id": safe_get(header, "identifier") or UNKNOWN_VALUE,
        "repository": repository_name,
        "identifier": normalize_identifier(ensure_list(safe_get(dc, "dc:identifier"))) or UNKNOWN_VALUE,
        "datestamp": safe_get(header, "datestamp") or UNKNOWN_VALUE,
        "setSpec": ensure_list(safe_get(header, "setSpec")) or UNKNOWN_VALUE,
        "title": normalize_title(safe_get(dc, "dc:title")) or UNKNOWN_VALUE,
        "creator": normalize_creator(safe_get(dc, "dc:creator")) or UNKNOWN_VALUE,
        "date": normalize_dates(ensure_list(safe_get(dc, "dc:date"))) or UNKNOWN_VALUE,
        "description": normalize_description(safe_get(dc, "dc:description"), repository_name) or UNKNOWN_VALUE,
        "subject": normalize_subject(ensure_list(safe_get(dc, "dc:subject"))) or UNKNOWN_VALUE,
        "rights": normalize_rights(ensure_list(safe_get(dc, "dc:rights"))) or UNKNOWN_VALUE,
        "format": normalize_format(ensure_list(safe_get(dc, "dc:format"))) or UNKNOWN_VALUE,
        "type": normalize_type(safe_get(dc, "dc:type")) or UNKNOWN_VALUE,
        "language": normalize_language(safe_get(dc, "dc:language")) or UNKNOWN_VALUE,
    }


def normalize_repository_name(filename: str) -> str:
    """Clean repository name extracted from filename."""
    clean = filename.replace("_-_", " - ")
    clean = clean.replace("_", " ")
    for bad, good in nm.repository_name_replacements.items():
        clean = clean.replace(bad, good)
    return clean.strip()


def process_file(path: str, global_seen_ids: set) -> Dict[str, Any]:
    """Process a JSON file containing OAI-PMH records and write outputs."""
    stats = {
        "valid": 0,
        "duplicates": 0,
        "unknown_title": 0,
        "skipped_file": False,
        "filename": os.path.basename(path),
        "total_records": 0,
        "title_stats": {"single": 0, "multi": 0, "unknown": 0},
    }

    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception as exc:
        print(f"[Error] reading file: {path}\n   → {exc}")
        stats["skipped_file"] = True
        return stats

    root = safe_get(raw, "OAI-PMH", {})
    list_records = safe_get(root, "ListRecords", {})
    records = ensure_list(safe_get(list_records, "record"))
    stats["total_records"] = len(records)

    raw_repo_name = os.path.basename(path).replace(".json", "")
    repository_name = normalize_repository_name(raw_repo_name)

    normalized_list: List[Dict[str, Any]] = []
    multititle_list: List[Dict[str, Any]] = []

    for index, rec in enumerate(records):
        # Title stats and handling of multi-title items
        try:
            raw_title = get_nested(rec, "metadata/oai_dc:dc/dc:title")
            if not raw_title:
                stats["title_stats"]["unknown"] += 1
            elif isinstance(raw_title, list) and len(raw_title) > 1:
                stats["title_stats"]["multi"] += 1
                multititle_list.append(rec)
                continue
            else:
                stats["title_stats"]["single"] += 1
        except Exception:
            stats["title_stats"]["unknown"] += 1

        try:
            normalized = normalize_record(rec, repository_name)

            if normalized["title"] == UNKNOWN_VALUE:
                stats["unknown_title"] += 1
                continue

            record_id = normalized["id"]
            if record_id != UNKNOWN_VALUE and record_id in global_seen_ids:
                stats["duplicates"] += 1
                continue
            if record_id != UNKNOWN_VALUE:
                global_seen_ids.add(record_id)

            normalized_list.append(normalized)
            stats["valid"] += 1
        except Exception as exc:
            print(f" [Error] processing record #{index} in file: {path}\n   → {exc}")
            continue

    # Write normalized results
    if normalized_list:
        out_path = os.path.join(NORMALIZED_DIR, os.path.basename(path).replace(".json", "_normalized.json"))
        try:
            with open(out_path, "w", encoding="utf-8") as fh:
                json.dump(normalized_list, fh, ensure_ascii=False, indent=2)
            print(f"[OK] Normalized: {out_path} ({len(normalized_list)} records)")
        except Exception as exc:
            print(f" [Error] writing output file: {out_path}\n   → {exc}")
    else:
        print(f"[SKIP] No valid records found in: {path}")
        stats["skipped_file"] = True

    # Save multi-title list
    if multititle_list:
        mt_path = os.path.join(MULTITITLE_DIR, os.path.basename(path).replace(".json", "_multititle.json"))
        try:
            with open(mt_path, "w", encoding="utf-8") as fh:
                json.dump(multititle_list, fh, ensure_ascii=False, indent=2)
            print(f"[INFO] Multi-title records saved: {mt_path} ({len(multititle_list)} records)")
        except Exception as exc:
            print(f" [Error] writing multi-title file: {mt_path}\n   → {exc}")

    return stats


def process_all_files() -> None:
    print("=" * 60)
    print("Starting OAI-PMH file normalization")
    print("=" * 60)

    global_seen_ids: set = set()
    skipped_files: List[str] = []
    empty_repos: List[str] = []
    duplicate_repos: List[str] = []
    title_stats_all: Dict[str, Any] = {}

    total_stats = {"valid": 0, "duplicates": 0, "unknown_title": 0}

    print(f"Reading from: {INPUT_DIR}")
    print(f"Writing to: {OUTPUT_DIR}")

    input_files: List[str] = []
    for root, _, files in os.walk(INPUT_DIR):
        for f in files:
            if f.endswith(".json"):
                input_files.append(os.path.join(root, f))

    for idx, path in enumerate(input_files, start=1):
        fname = os.path.basename(path)
        print(f"\n[{idx}] Processing: {fname}")
        stats = process_file(path, global_seen_ids)

        repo_name = stats["filename"].replace(".json", "")
        title_stats_all[repo_name] = stats["title_stats"]
        title_stats_all[repo_name]["total"] = stats["total_records"]

        total_stats["valid"] += stats["valid"]
        total_stats["duplicates"] += stats["duplicates"]
        total_stats["unknown_title"] += stats["unknown_title"]

        if stats["skipped_file"]:
            skipped_files.append(fname)
            if stats["total_records"] > 0 and stats["duplicates"] == stats["total_records"]:
                duplicate_repos.append(fname)
            else:
                empty_repos.append(fname)
        else:
            if stats["valid"] == 0:
                if stats["total_records"] > 0 and stats["duplicates"] == stats["total_records"]:
                    duplicate_repos.append(fname)
                else:
                    empty_repos.append(fname)

    report = {
        "total_files": len(input_files),
        "processed_files": len(input_files) - len(skipped_files),
        "skipped_files": len(skipped_files),
        "total_records_saved": total_stats["valid"],
        "empty_repos": empty_repos,
        "duplicate_repos": duplicate_repos,
        "title_stats": title_stats_all,
    }

    generate_pdf_report(report, os.path.join(OUTPUT_DIR, "normalization_report.pdf"))
    print(f"Processed files: {len(input_files)}")


if __name__ == "__main__":
    process_all_files()