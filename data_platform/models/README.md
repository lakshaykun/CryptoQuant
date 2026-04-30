# Models

This package contains the machine learning lifecycle: feature definitions, data loading, training, evaluation, inference, and model loading.

## Package map

- [config/](config/) - feature lists, model parameters, and artifact paths.
- [data/](data/) - data loading, schema checks, and train/test splitting.
- [features/](features/) - reusable feature engineering logic.
- [training/](training/) - training pipeline and trainer implementation.
- [evaluation/](evaluation/) - metrics and backtesting helpers.
- [inference/](inference/) - batch and real-time prediction wrappers.
- [registry/](registry/) - local model loading and local artifact persistence.
- [targets/](targets/) - target generation definitions.

## Lifecycle

1. Load market data from the curated storage layer.
2. Validate the schema and split data in time order.
3. Train a model using the feature set defined in `config/model_config.py`.
4. Evaluate predictions with error and directional metrics.
5. Save the artifact locally or register it for later serving.
6. Reuse the same feature contract in `api/` for online prediction.

## Future direction

- Keep training and serving feature definitions in one place.
- Add experiment tracking, registry promotion, and model version metadata.
- Expand the package toward multi-horizon or multi-asset model families.
