# UK Sanctions List Transformation

## Overview

This repository contains a Python transformation pipeline for preparing the UK
financial sanctions list into structured CSV outputs suitable for customer
screening and comparison. The deliverable is intentionally split into one
entity-level file for review and one name-level file for matching.

## Getting the Code

To run this project locally, first download the repository from GitHub.

### Option 1: Clone using Git

If you have Git installed, run:

```bash
git clone https://github.com/beep-sketch/malverde_task.git
cd malverde_task
```

### Option 2: Download as ZIP

- Go to the GitHub repository page
- Click the green **Code** button
- Select **Download ZIP**
- Extract the files and navigate into the project folder

## Repository Contents

- `UK-Sanctions-List.csv`: raw source data
- `transform_sanctions.py`: executable transformation script
- `sanctions_notebook.ipynb`: supporting notebook with exploratory analysis
- `notebook_output/`: generated output folders
- `Data_Quality_Notes.md`: separate write-up covering data quality findings and output assessment

## Input File Name

By default, the script expects the raw sanctions file to be named exactly:

- `UK-Sanctions-List.csv`

and placed in the project root.

If the input file has a different name or location, run the script with the
`--input` argument.

## Requirements

- Python 3.9+
- `pandas`

Install the dependency with:

```bash
pip install pandas
```

## How To Run

Run with default paths:

```bash
python3 transform_sanctions.py
```

This default command assumes the source file is:

```bash
UK-Sanctions-List.csv
```

Run with explicit paths:

```bash
python3 transform_sanctions.py --input UK-Sanctions-List.csv --output-dir notebook_output
```

## Output Files

Each run creates a new timestamped folder inside `notebook_output/`, for example:

- `notebook_output/output_May-03_1524/`

Each run folder contains:

- `sanctioned_parties_master.csv`
- `sanctioned_names.csv`
- `run_summary.json`

The two CSV outputs serve different purposes:

- `sanctioned_parties_master.csv`: one row per sanctioned entity for entity-level review and aggregated comparison
- `sanctioned_names.csv`: one row per screening name or alias for name matching and alias coverage

Both are kept because customer screening usually starts with name matching, but
possible hits still need to be reviewed at full entity level.


## Key Insights

- The raw sanctions file is heavily denormalised, so one sanctioned party can appear across many rows.
- Name reconstruction is essential because `Name 1` alone is not a reliable completeness measure.
- Identifier coverage is limited, so screening should rely primarily on name matching supported by contextual fields such as country, address, and DOB evidence.
- The two-output design is intentional: the names file supports screening, while the master file supports full-entity review with explicit contextual fields.

## Notes

- The source CSV uses line 1 for the report date and line 2 for the actual header row.
- The notebook is included as supporting analysis, but `transform_sanctions.py` is the main executable deliverable.
- The intended final submission output folder is `notebook_output/output_May-04_1721/`.
