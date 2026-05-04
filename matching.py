import argparse
import csv
import json
import re
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd


NAME_PART_COLUMNS = ["Title", "Name 1", "Name 2", "Name 3", "Name 4", "Name 5", "Name 6"]

ADDRESS_COLUMNS = [
    "Address Line 1",
    "Address Line 2",
    "Address Line 3",
    "Address Line 4",
    "Address Line 5",
    "Address Line 6",
    "Address Postal Code",
    "Address Country",
]

ASSOCIATED_COUNTRY_COLUMNS = [
    "Address Country",
    "Country of birth",
    "Nationality(/ies)",
    "Current believed flag of ship",
    "Previous flags",
]

ENTITY_DETAIL_COLUMNS = [
    "Type of entity",
    "Business registration number (s)",
    "Subsidiaries",
    "Parent company",
    "IMO number",
    "Type of ship",
    "Current owner/operator (s)",
    "Previous owner/operator (s)",
    "Current believed flag of ship",
    "Previous flags",
    "Tonnage of ship",
    "Length of ship",
    "Year Built",
    "Hull identification number (HIN)",
]

REQUIRED_COLUMNS = sorted(
    set(
        NAME_PART_COLUMNS
        + ADDRESS_COLUMNS
        + ASSOCIATED_COUNTRY_COLUMNS
        + ENTITY_DETAIL_COLUMNS
        + [
            "OFSI Group ID",
            "Unique ID",
            "Name type",
            "Name non-latin script",
            "D.O.B",
            "Passport number",
            "National Identifier number",
            "Sanctions Imposed",
            "Designation Type",
            "Alias strength",
        ]
    )
)

RAW_DATE_RE = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
YEAR_RE = re.compile(r"(?<!\d)(?:18|19|20)\d{2}(?!\d)")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform the UK sanctions list into entity and screening-name outputs."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("UK-Sanctions-List.csv"),
        help="Path to the source OFSI CSV file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("notebook_output"),
        help="Base directory where a timestamped run folder will be created.",
    )
    parser.add_argument(
        "--check-name",
        type=str,
        default="",
        help="Optional entity name to screen against the sanctions names output.",
    )
    return parser.parse_args()


def normalize_space(value):
    text = "" if value is None else str(value)
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_name_type(value):
    """Map inconsistent source name labels into a small controlled vocabulary."""
    text = normalize_space(value).casefold()
    if not text:
        return "unknown"

    mapping = {
        "primary name": "primary_name",
        "primary name variation": "primary_name_variation",
        "alias": "alias",
    }
    return mapping.get(text, text.replace(" ", "_"))


def normalize_for_matching(value):
    """Create a simple canonical version of names for exact/fuzzy matching inputs."""
    text = normalize_space(value).casefold()
    text = re.sub(r"[^\w\s]", " ", text)
    return normalize_space(text)


def parse_raw_date(value):
    """Convert DD/MM/YYYY dates to ISO format; keep unparseable dates blank."""
    text = normalize_space(value)
    match = RAW_DATE_RE.match(text)
    if not match:
        return ""
    day, month, year = match.groups()
    return f"{year}-{month}-{day}"


def extract_years(values):
    """Extract year-level DOB evidence from complete or partial date strings."""
    seen = set()
    years = []
    for value in values:
        for year in YEAR_RE.findall(str(value)):
            if year not in seen:
                seen.add(year)
                years.append(year)
    return " | ".join(years)


def first_nonblank(values):
    """Return the first populated value from an ordered list of source values."""
    for value in values:
        text = normalize_space(value)
        if text:
            return text
    return ""


def unique_values(values, *, split_pipes=False):
    """Return unique non-blank values while preserving their first-seen order."""
    seen = set()
    output = []
    for value in values:
        parts = str(value).split("|") if split_pipes else [value]
        for part in parts:
            text = normalize_space(part)
            if not text:
                continue
            key = text.casefold()
            if key not in seen:
                seen.add(key)
                output.append(text)
    return output


def unique_join(values, *, split_pipes=False):
    """Join de-duplicated values using a visible delimiter for multi-value fields."""
    return " | ".join(unique_values(values, split_pipes=split_pipes))


def column_slug(column_name):
    """Convert source column names into safer output column names."""
    return (
        column_name.lower()
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "_")
    )


def safe_filename_part(value):
    """Convert user input into a safe filename component."""
    text = normalize_space(value)
    text = re.sub(r"[^A-Za-z0-9_-]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_") or "entity"


def read_report_date(input_path):
    """Read the report date stored in the first metadata row of the OFSI CSV."""
    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return handle.readline().strip()


def make_run_output_dir(base_dir):
    """Create a timestamped output folder without overwriting earlier runs."""
    base_dir.mkdir(parents=True, exist_ok=True)
    folder_name = datetime.now().strftime("output_%b-%d_%H%M")
    run_dir = base_dir / folder_name

    if not run_dir.exists():
        run_dir.mkdir()
        return run_dir

    counter = 1
    while True:
        candidate = base_dir / f"{folder_name}_{counter:02d}"
        if not candidate.exists():
            candidate.mkdir()
            return candidate
        counter += 1


def validate_columns(raw):
    """Fail early if the source schema is missing fields used by the pipeline."""
    missing = [column for column in REQUIRED_COLUMNS if column not in raw.columns]
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"Input file is missing required columns: {missing_text}")


def load_raw(input_path):
    """Load the OFSI CSV and create cleaned helper fields used downstream."""
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    raw = pd.read_csv(input_path, skiprows=1, dtype=str, keep_default_na=False)
    validate_columns(raw)

    raw_row_count = len(raw)
    duplicate_row_count = int(raw.duplicated().sum())

    for column in raw.columns:
        raw[column] = raw[column].map(normalize_space)

    raw["record_id"] = raw["OFSI Group ID"].where(raw["OFSI Group ID"] != "", raw["Unique ID"])
    raw["name_type_normalized"] = raw["Name type"].map(normalize_name_type)

    raw["full_name"] = raw[NAME_PART_COLUMNS].apply(
        lambda row: " ".join(part for part in row if part),
        axis=1,
    )

    raw["full_address"] = raw[ADDRESS_COLUMNS].apply(
        lambda row: ", ".join(part for part in row if part),
        axis=1,
    )

    cleaned = raw.drop_duplicates().copy()

    load_stats = {
        "raw_rows_before_deduplication": int(raw_row_count),
        "exact_duplicate_rows_removed": duplicate_row_count,
        "raw_rows_after_deduplication": int(len(cleaned)),
        "rows_missing_ofsi_group_id": int((raw["OFSI Group ID"] == "").sum()),
    }

    return cleaned, load_stats


def collect_associated_countries(group_values):
    """Collect all country-like values that may help support screening review."""
    countries = []
    for column in ASSOCIATED_COUNTRY_COLUMNS:
        countries.extend(group_values[column])
    return countries


def build_master(raw):
    """Build the entity-level file: one row per sanctioned party."""
    master_rows = []

    for record_id, group in raw.groupby("record_id", sort=True):
        group_values = {column: group[column].tolist() for column in group.columns}

        primary_mask = group["name_type_normalized"].isin(
            ["primary_name", "primary_name_variation"]
        )
        alias_mask = group["name_type_normalized"].isin(["alias", "primary_name_variation"])

        all_names = unique_values(
            group["full_name"].tolist() + group["Name non-latin script"].tolist()
        )
        primary_names = unique_values(group.loc[primary_mask, "full_name"].tolist())
        alias_names = unique_values(
            group.loc[alias_mask, "full_name"].tolist()
            + group.loc[alias_mask, "Name non-latin script"].tolist()
        )
        associated_countries = collect_associated_countries(group_values)

        ofsi_group_id = first_nonblank(group_values["OFSI Group ID"])
        unique_id = first_nonblank(group_values["Unique ID"])
        key_source = "OFSI Group ID" if ofsi_group_id else "Unique ID"
        primary_name = primary_names[0] if primary_names else (all_names[0] if all_names else "")

        row = {
            "record_id": record_id,
            "ofsi_group_id": ofsi_group_id,
            "unique_id": unique_id,
            "key_source": key_source,
            "primary_name": primary_name,
            "primary_name_normalized": normalize_for_matching(primary_name),
            "all_names": " | ".join(all_names),
            "all_aliases": " | ".join(alias_names),
            "date_of_births_raw": unique_join(group_values["D.O.B"]),
            "date_of_births": unique_join([parse_raw_date(value) for value in group_values["D.O.B"]]),
            "dob_years": extract_years(group_values["D.O.B"]),
            "nationalities": unique_join(group_values["Nationality(/ies)"]),
            "countries_of_birth": unique_join(group_values["Country of birth"]),
            "address_countries": unique_join(group_values["Address Country"]),
            "associated_countries": unique_join(associated_countries),
            "addresses": unique_join(group_values["full_address"]),
            "passport_numbers": unique_join(group_values["Passport number"]),
            "national_identifier_numbers": unique_join(group_values["National Identifier number"]),
            "sanctions_imposed": unique_join(group_values["Sanctions Imposed"], split_pipes=True),
            "designation_type": first_nonblank(group_values["Designation Type"]),
            "source_row_count": int(len(group)),
        }

        for column in ENTITY_DETAIL_COLUMNS:
            row[column_slug(column)] = unique_join(group_values[column])

        master_rows.append(row)

    return pd.DataFrame(master_rows)


def build_names(raw):
    """Build the screening-name file: one row per usable name."""
    names = raw[(raw["full_name"] != "") | (raw["Name non-latin script"] != "")].copy()

    names["ofsi_group_id"] = names["OFSI Group ID"]
    names["name_for_screening"] = names["full_name"].where(
        names["full_name"] != "",
        names["Name non-latin script"],
    )
    names["name_for_screening_normalized"] = names["name_for_screening"].map(normalize_for_matching)
    names["date_of_birth_iso"] = names["D.O.B"].map(parse_raw_date)
    names["date_of_birth_raw"] = names["D.O.B"]
    names["dob_year"] = names["D.O.B"].apply(lambda value: extract_years([value]))

    screening_columns = [
        "record_id",
        "ofsi_group_id",
        "Unique ID",
        "name_for_screening",
        "name_for_screening_normalized",
        "name_type_normalized",
        "Alias strength",
        "D.O.B",
        "date_of_birth_raw",
        "date_of_birth_iso",
        "dob_year",
        "Nationality(/ies)",
        "Country of birth",
        "Address Country",
        "Designation Type",
    ]

    names = names[screening_columns].drop_duplicates(
        subset=[
            "record_id",
            "name_type_normalized",
            "Alias strength",
            "name_for_screening",
            "D.O.B",
        ]
    )

    names = names.drop(columns=["D.O.B"]).rename(
        columns={
            "Unique ID": "unique_id",
            "Alias strength": "alias_strength",
            "Nationality(/ies)": "nationalities",
            "Country of birth": "country_of_birth",
            "Address Country": "address_country",
            "Designation Type": "designation_type",
        }
    )

    return names[
        [
            "record_id",
            "ofsi_group_id",
            "unique_id",
            "name_for_screening",
            "name_for_screening_normalized",
            "name_type_normalized",
            "alias_strength",
            "date_of_birth_raw",
            "date_of_birth_iso",
            "dob_year",
            "nationalities",
            "country_of_birth",
            "address_country",
            "designation_type",
        ]
    ].reset_index(drop=True)


def build_quality_summary(raw, master, names, load_stats):
    """Create reproducible quality metrics for the run summary JSON."""
    master_records = len(master)

    missing_dob_entities = int((master["date_of_births_raw"] == "").sum())
    missing_parsed_dob_entities = int((master["date_of_births"] == "").sum())
    missing_address_entities = int((master["addresses"] == "").sum())
    missing_identifier_entities = int(
        (
            (master["passport_numbers"] == "")
            & (master["national_identifier_numbers"] == "")
            & (master["business_registration_number_s"] == "")
            & (master["imo_number"] == "")
        ).sum()
    )

    def pct(count, total):
        return round((count / total) * 100, 2) if total else 0.0

    return {
        **load_stats,
        "master_records": int(master_records),
        "screening_name_rows": int(len(names)),
        "entities_missing_dob_raw": missing_dob_entities,
        "entities_missing_dob_raw_pct": pct(missing_dob_entities, master_records),
        "entities_missing_fully_parsed_dob": missing_parsed_dob_entities,
        "entities_missing_fully_parsed_dob_pct": pct(missing_parsed_dob_entities, master_records),
        "entities_missing_address": missing_address_entities,
        "entities_missing_address_pct": pct(missing_address_entities, master_records),
        "entities_missing_key_identifiers": missing_identifier_entities,
        "entities_missing_key_identifiers_pct": pct(missing_identifier_entities, master_records),
        "distinct_normalized_name_types": sorted(raw["name_type_normalized"].unique().tolist()),
    }


def score_name_match(user_name_normalized, sanctions_name_normalized):
    """Score a user name against a sanctions name."""
    if not user_name_normalized or not sanctions_name_normalized:
        return 0.0

    if user_name_normalized == sanctions_name_normalized:
        return 100.0

    if user_name_normalized in sanctions_name_normalized:
        return 90.0

    if sanctions_name_normalized in user_name_normalized:
        return 85.0

    return round(
        SequenceMatcher(None, user_name_normalized, sanctions_name_normalized).ratio() * 100,
        2,
    )


def run_entity_name_check(entity_name, names, master, output_dir):
    """
    Check a user-provided entity name against sanctions screening names.

    The function writes a timestamped text report into the current run output folder.
    """
    entity_name = normalize_space(entity_name)
    if not entity_name:
        return None

    entity_name_normalized = normalize_for_matching(entity_name)

    scored = names.copy()
    scored["match_score"] = scored["name_for_screening_normalized"].apply(
        lambda sanctions_name: score_name_match(entity_name_normalized, sanctions_name)
    )

    matches = scored[scored["match_score"] >= 80].copy()
    matches = matches.sort_values(
        by=["match_score", "name_type_normalized", "name_for_screening"],
        ascending=[False, True, True],
    ).head(25)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_entity_name = safe_filename_part(entity_name)
    report_path = output_dir / f"{safe_entity_name}_{timestamp}.txt"

    lines = [
        "Sanctions Entity Name Check",
        "=" * 28,
        f"Input entity name: {entity_name}",
        f"Normalized input: {entity_name_normalized}",
        f"Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "Match logic:",
        "- 100 = exact normalized name match",
        "- 90 = input name appears inside sanctions name",
        "- 85 = sanctions name appears inside input name",
        "- Other scores use a simple text similarity ratio",
        "- Report includes matches scoring 80 or above",
        "",
    ]

    if matches.empty:
        lines.extend(
            [
                "Result: No possible sanctions name matches found at score >= 80.",
                "",
                "Important: This is a simple name-only screening check. It should not be treated",
                "as a final sanctions decision without manual review and other identifiers.",
            ]
        )
    else:
        lines.extend(
            [
                f"Result: {len(matches)} possible match(es) found at score >= 80.",
                "",
                "Possible matches:",
                "-" * 17,
            ]
        )

        master_lookup = master.set_index("record_id", drop=False)

        for index, match in matches.iterrows():
            record_id = match["record_id"]
            master_row = master_lookup.loc[record_id] if record_id in master_lookup.index else {}

            lines.extend(
                [
                    "",
                    f"Match score: {match['match_score']}",
                    f"Matched sanctions name: {match['name_for_screening']}",
                    f"Name type: {match['name_type_normalized']}",
                    f"Alias strength: {match['alias_strength']}",
                    f"Record ID: {record_id}",
                    f"OFSI Group ID: {match['ofsi_group_id']}",
                    f"Unique ID: {match['unique_id']}",
                    f"DOB raw: {match['date_of_birth_raw']}",
                    f"DOB ISO: {match['date_of_birth_iso']}",
                    f"DOB year: {match['dob_year']}",
                    f"Nationalities: {match['nationalities']}",
                    f"Country of birth: {match['country_of_birth']}",
                    f"Address country: {match['address_country']}",
                    f"Designation type: {match['designation_type']}",
                ]
            )

            if isinstance(master_row, pd.Series):
                lines.extend(
                    [
                        f"Primary name: {master_row.get('primary_name', '')}",
                        f"All names: {master_row.get('all_names', '')}",
                        f"All aliases: {master_row.get('all_aliases', '')}",
                        f"Associated countries: {master_row.get('associated_countries', '')}",
                        f"Sanctions imposed: {master_row.get('sanctions_imposed', '')}",
                        f"Addresses: {master_row.get('addresses', '')}",
                    ]
                )

        lines.extend(
            [
                "",
                "Important: This is a simple name-only screening check. It should not be treated",
                "as a final sanctions decision without manual review and other identifiers.",
            ]
        )

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Entity name check report: {report_path}")

    return report_path


def write_outputs(master, names, output_dir, report_date, quality_summary):
    """Write final CSV outputs and a JSON run summary."""
    master_path = output_dir / "sanctioned_parties_master.csv"
    names_path = output_dir / "sanctioned_names.csv"
    summary_path = output_dir / "run_summary.json"

    master.to_csv(master_path, index=False, quoting=csv.QUOTE_MINIMAL)
    names.to_csv(names_path, index=False, quoting=csv.QUOTE_MINIMAL)

    summary = {
        "report_date": report_date,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        **quality_summary,
    }

    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"Report date: {report_date}")
    print(f"Master output: {master_path}")
    print(f"Name output: {names_path}")
    print(f"Run summary: {summary_path}")


def main():
    args = parse_args()

    report_date = read_report_date(args.input)
    raw, load_stats = load_raw(args.input)

    master = build_master(raw)
    names = build_names(raw)
    quality_summary = build_quality_summary(raw, master, names, load_stats)

    output_dir = make_run_output_dir(args.output_dir)
    write_outputs(master, names, output_dir, report_date, quality_summary)

    if args.check_name:
        run_entity_name_check(args.check_name, names, master, output_dir)


if __name__ == "__main__":
    main()