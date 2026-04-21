from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LAYERS = ("bronze", "silver", "gold")


def discover_parquet_files(input_path: Path) -> list[Path]:
    """Return parquet files from a file path or recursively from a directory."""
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


def export_layer_to_csv(layer: str, input_dir: Path, output_file: Path) -> None:
    parquet_files = discover_parquet_files(input_dir)
    if not parquet_files:
        print(f"[{layer}] No parquet files found at: {input_dir}")
        return

    merged_df = read_parquet_files(parquet_files)
    if merged_df.empty:
        print(f"[{layer}] No readable parquet data to export.")
        return

    output_file.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(output_file, index=False)

    print(
        f"[{layer}] Exported {len(merged_df)} rows from "
        f"{len(parquet_files)} parquet files to {output_file}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export all parquet files from Bronze/Silver/Gold layer directories "
            "to CSV files."
        )
    )
    parser.add_argument(
        "--delta-root",
        default=str(PROJECT_ROOT / "delta"),
        help="Root folder containing bronze, silver, and gold directories.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "delta"),
        help="Directory where layer CSV files will be written.",
    )
    parser.add_argument(
        "--layers",
        default=",".join(DEFAULT_LAYERS),
        help="Comma-separated layers to export (default: bronze,silver,gold).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    delta_root = Path(args.delta_root)
    output_dir = Path(args.output_dir)
    layers = [layer.strip().lower() for layer in str(args.layers).split(",") if layer.strip()]

    if not layers:
        raise ValueError("No layers provided. Use --layers bronze,silver,gold")

    for layer in layers:
        input_dir = delta_root / layer
        output_file = output_dir / f"{layer}.csv"
        export_layer_to_csv(layer, input_dir, output_file)


if __name__ == "__main__":
    main()
