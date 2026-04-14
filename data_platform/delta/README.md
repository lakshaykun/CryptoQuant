# Medallion Storage

This directory is the Delta Lake storage layer for the project. It is not source code; it holds the Bronze, Silver, and Gold datasets written by the pipelines.

## Layers

- Bronze - raw normalized market records with ingestion metadata.
- Silver - cleaned and standardized market records.
- Gold - feature-rich market records used for training and inference.

## Operational notes

- Partitioning is centered on `symbol` and `date`.
- Checkpoint locations live alongside the data layout for batch and streaming jobs.
- The storage layout should remain stable so training and serving code can rely on a predictable contract.

## Future direction

- Add retention and vacuum policies for long-running deployments.
- Formalize versioning and reproducibility for feature tables.