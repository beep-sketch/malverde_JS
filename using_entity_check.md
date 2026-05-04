# entity_check.py Usage Guide

## Overview

`entity_check.py` checks a user-provided entity name against previously transformed UK sanctions data.

It reads from:

- `sanctioned_names.csv`
- `sanctioned_parties_master.csv`

It then creates a text report with any matches found.

## Requirements

- Python 3.x
- `pandas` installed
- A completed run of `transform_sanctions.py`

## How to Run

From your terminal:

```bash
python entity_check.py
```

The program will ask for:

1. Output folder path

Example:

```text
notebook_output/output_May-04_1530
```

2. Entity name to check

Example:

```text
Vladimir Putin
```

## What the Script Does

1. Loads the sanctions datasets from the folder you provide.
2. Normalises the input name.
3. Compares it against `name_for_screening_normalized`.
4. Finds matching `record_id` values.
5. Uses those IDs to retrieve full entity details from the master file.
6. Creates a text report.

## Output

The report is saved in:

```text
<your_output_folder>/entity_check/
```

The file name format is:

```text
<Entity_Name>_YYYYMMDD_HHMMSS.txt
```

Example:

```text
Vladimir_Putin_20260504_153012.txt
```

## Notes

- Matching is simple substring-based matching.
- It does not use fuzzy matching.
- Results should be treated as potential matches only.
- Manual review is required before making any decision.

## Limitations

- Does not handle spelling mistakes well.
- Does not calculate a match score.
- Does not compare extra identifiers like date of birth or nationality.

## Future Improvements

- Add fuzzy matching.
- Add match confidence scoring.
- Add checks using date of birth and country.