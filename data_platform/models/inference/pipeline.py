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
        return self.model_config.get("features", [])

    def _prepare_features(self, df):
        configured = self.model_config.get("features", [])
        base = [col for col in configured if col in df.columns]
        X = df[base].copy()

        if "symbol" in X.columns and not pd.api.types.is_numeric_dtype(X["symbol"]):
            symbols = data_config.get("symbols") or []
            symbol_map = {symbol: index for index, symbol in enumerate(symbols)}
            X["symbol"] = X["symbol"].map(symbol_map).fillna(-1).astype(int)

        return X.reindex(columns=self.feature_columns, fill_value=0)

    def run(self, df):
        X = self._prepare_features(df)
        if X.empty:
            raise ValueError("No engineered feature rows provided")
        outputs = {}
        confidence_threshold = float(
            self.model_config.get("inference", {}).get("confidence_threshold", 0.0)
        )
        for task_name, model in self.models.items():
            raw = np.asarray(model.predict(X)).reshape(-1)
            if task_name.startswith("sign_"):
                pred = np.rint(raw).astype(int)
                pred = np.clip(pred, -1, 1)
                if confidence_threshold > 0:
                    pred = np.where(np.abs(raw) < confidence_threshold, 0, pred)
                outputs[task_name] = pred.tolist()
            else:
                outputs[task_name] = raw.astype(float).tolist()
        return {"predictions": outputs}
