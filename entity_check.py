import re
from datetime import datetime
from pathlib import Path

import pandas as pd


def normalize(value):
    text = "" if value is None else str(value)
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def safe_filename(value):
    text = re.sub(r"[^A-Za-z0-9]+", "_", value)
    return text.strip("_")


def main():
    
    folder_input = input("Enter output folder path: ").strip()
    name_input = input("Enter entity name to check: ").strip()

    folder = Path(folder_input)

    names_path = folder / "sanctioned_names.csv"
    master_path = folder / "sanctioned_parties_master.csv"

    if not names_path.exists() or not master_path.exists():
        print("Error: required files not found in folder")
        return

    
    names = pd.read_csv(names_path)
    master = pd.read_csv(master_path)

    input_norm = normalize(name_input)

    
    names["match"] = names["name_for_screening_normalized"].apply(
        lambda x: input_norm in str(x)
    )

    matches = names[names["match"]]

    if matches.empty:
        print("No match found")
        return

    record_ids = matches["record_id"].unique()
    matched_entities = master[master["record_id"].isin(record_ids)]

   
    output_dir = folder / "entity_check"
    output_dir.mkdir(exist_ok=True)

    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_filename(name_input)}_{timestamp}.txt"
    output_file = output_dir / filename

    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"Input name: {name_input}\n")
        f.write(f"Normalized: {input_norm}\n")

        f.write("\n=== MATCHED NAMES ===\n")
        for _, row in matches.iterrows():
            f.write(f"{row['name_for_screening']} (record_id: {row['record_id']})\n")

        f.write("\n=== ENTITY DETAILS ===\n")
        for _, row in matched_entities.iterrows():
            f.write("\n-------------------------\n")
            for col in row.index:
                f.write(f"{col}: {row[col]}\n")

    print(f"Report written to: {output_file}")


if __name__ == "__main__":
    main()