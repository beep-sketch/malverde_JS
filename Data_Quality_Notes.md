# Data Quality And Output Assessment

## Purpose

This note documents the main data quality findings from the UK sanctions source
file and explains how the final output structure supports the matching use case
described in the assignment brief.

## Data Quality Assessment

The source dataset is heavily denormalised. The same sanctioned party often
appears across multiple rows because names, aliases, addresses, and other
attributes are repeated across the file.

Key findings:

- `57,033` raw rows collapse to `6,035` unique entities after grouping by the entity key.
- There are `650` exact duplicate rows in the raw data.
- `2,264` rows are missing `OFSI Group ID`, so fallback to `Unique ID` is required to build a stable entity key.
- `Name 1` is blank on `33,142` rows, so it is not a reliable standalone completeness measure.
- After reconstructing names across `Title` and `Name 1` to `Name 6`, there are `0` entities with no usable name.
- `32,224` rows have no DOB, and only `16,585` rows contain a fully parseable DOB.
- At entity level, `45.93%` of entities have no DOB and `55.15%` have no fully parsed DOB.
- `47.97%` of entities have no usable aggregated address.
- `68.17%` of entities have no passport number, national identifier, business registration number, or IMO number.
- The raw file contains inconsistent `Name type` values such as `Primary Name`, `Primary name`, `Primary Name Variation`, `Primary name variation`, `Alias`, and `ALias`.

## Implications For Screening

These quality issues matter for customer screening:

- Names are the strongest matching signal once reconstructed correctly.
- Deterministic identifiers are too sparse to be the primary screening key.
- DOB is useful as supporting evidence, but not sufficiently complete to drive matching alone.
- Address and country fields are useful contextual enrichments, but they are not available for every entity.

This means screening should rely primarily on name-based matching, supported by
country, address, DOB evidence, and any available identifiers.

## Final Output Assessment

The final outputs are designed around two different screening needs.

### `sanctioned_parties_master.csv`

Purpose:

- one row per sanctioned entity
- entity-level review and aggregation
- reference file for reviewing a possible match in full context

Main fields include:

- entity identifiers
- primary and aggregated names
- raw and parsed DOB fields
- explicit `nationalities`, `countries_of_birth`, and `address_countries`
- associated countries
- aggregated addresses
- identifiers such as passport, national ID, business registration, and IMO
- organisation and vessel context fields

Notes:

- good fit for entity-level review
- one row per entity makes it easier to compare high-level customer context
- some fields are pipe-delimited because the source is multi-valued and denormalised

### `sanctioned_names.csv`

Purpose:

- one row per screening name or alias
- exact or fuzzy name matching
- preservation of aliases, transliterations, and primary name variations

Main fields include:

- entity identifiers
- `name_for_screening`
- `name_type_normalized`
- DOB evidence
- nationality and country context
- designation type

Notes:

- better suited than the master file for operational name screening
- avoids hiding aliases inside a single aggregated field
- blank source name types are preserved as `unknown` when the name itself is still usable for screening

## Why Two CSV Files Were Kept

The two-file design is intentional:

- `sanctioned_names.csv` is the matching-oriented file
- `sanctioned_parties_master.csv` is the entity review file

If everything were collapsed into one file, either alias coverage would become
harder to use for screening or entity-level context would become repetitive and
less clear.


