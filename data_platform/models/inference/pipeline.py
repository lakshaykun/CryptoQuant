import pandas as pd
import numpy as np
import joblib
from pathlib import Path

from utils_global.config_loader import load_config


data_config = load_config("configs/data.yaml")


class InferencePipeline:
    def __init__(self, models, model_config):
        self.models = models
        self.model_config = model_config
        self.feature_columns = self._load_feature_columns()

    def _load_feature_columns(self):
        artifact_path = Path("models/artifacts/feature_columns.pkl")
        if artifact_path.exists():
            return joblib.load(artifact_path)
        return self.model_config.get("features_long", self.model_config.get("features_short", []))

    def _prepare_features(self, df, task_features):
        base = [col for col in task_features if col in df.columns]
        X = df[base].copy()

        if "symbol" in X.columns and not pd.api.types.is_numeric_dtype(X["symbol"]):
            symbols = data_config.get("symbols") or []
            symbol_map = {symbol: index for index, symbol in enumerate(symbols)}
            X["symbol"] = X["symbol"].map(symbol_map).fillna(-1).astype(int)

        return X.reindex(columns=task_features, fill_value=0)

    def run(self, df):
        outputs = {}
        confidence_threshold = float(
            self.model_config.get("inference", {}).get("confidence_threshold", 0.0)
        )
        for task_name, model in self.models.items():
            task_cfg = self.model_config.get("models", {}).get(task_name, {})
            task_features_key = "features_long" if task_cfg.get("horizon") == "1d" else "features_short"
            task_features = self.model_config.get(task_features_key, self.model_config.get("features_short", self.model_config.get("features", [])))

            X = self._prepare_features(df, task_features)
            if X.empty:
                raise ValueError("No engineered feature rows provided")

            raw = np.asarray(model.predict(X)).reshape(-1)
            if task_name.startswith("sign_"):
                pred = np.rint(raw).astype(int)
                pred = np.clip(pred, 0, 2)
                if confidence_threshold > 0:
                    pred = np.where(np.abs(raw) < confidence_threshold, 1, pred)
                outputs[task_name] = pred.tolist()
            else:
                outputs[task_name] = raw.astype(float).tolist()
        return {"predictions": outputs}
