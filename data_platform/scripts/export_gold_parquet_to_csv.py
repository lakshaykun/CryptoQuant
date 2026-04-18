from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def discover_parquet_files(input_path: Path) -> list[Path]:
    """Return parquet files from a parquet file path or a layer directory."""
    if input_path.is_file():
        return [input_path] if input_path.suffix == ".parquet" else []

    if not input_path.exists():
        return []

    return sorted(
        path
        for path in input_path.rglob("*.parquet")
        if "_delta_log" not in path.parts
    )


def read_parquet_files(parquet_files: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for parquet_file in parquet_files:
        try:
            frame = pd.read_parquet(parquet_file)
            if not frame.empty:
                frames.append(frame)
        except Exception as exc:
            print(f"Skipping unreadable parquet file {parquet_file}: {exc}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def export_gold_to_csv(input_path: Path, output_csv: Path) -> None:
    parquet_files = discover_parquet_files(input_path)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found at: {input_path}")

    merged_df = read_parquet_files(parquet_files)
    if merged_df.empty:
        raise ValueError("No readable parquet data found to export.")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(output_csv, index=False)

    print(
        f"Exported {len(merged_df)} rows from {len(parquet_files)} parquet files to {output_csv}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read parquet data from the gold layer and export it as CSV."
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Parquet file path or directory containing gold layer parquet files.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV file path (default: delta/gold.csv).",
    )
    args = parser.parse_args()

    input_path = (
        Path(args.input)
        if args.input
        else PROJECT_ROOT / "delta" / "gold"
    )
    output_csv = (
        Path(args.output)
        if args.output
        else PROJECT_ROOT / "delta" / "gold.csv"
    )

    export_gold_to_csv(input_path, output_csv)


if __name__ == "__main__":
    main()
