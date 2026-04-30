# models/training/trainer.py

from sklearn.preprocessing import StandardScaler
from models.evaluation.evaluate import evaluate_model
import xgboost as xgb
from sklearn.ensemble import RandomForestRegressor
from catboost import CatBoostRegressor
import lightgbm as lgb
from sklearn.svm import SVR
import pandas as pd
from models.registry.local_registry import save_metrics, save_model, save_scaler

class ModelInfo:
    def __init__(self, name, train_func, params_config):
        self.name = name
        self.train_func = train_func
        self.params = params_config.get(name, {})

class Trainer:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        params = self.config.get("model_params", {})
        self.models = [
            ModelInfo(
                name="xgboost",
                train_func=self._train_xgboost,
                params_config=params
            ),
            ModelInfo(
                name="random_forest",
                train_func=self._train_random_forest,
                params_config=params
            ),
            ModelInfo(
                name="catboost",
                train_func=self._train_catboost,
                params_config=params
            ),
            ModelInfo(
                name="lightgbm",
                train_func=self._train_lightgbm,
                params_config=params
            ),
            ModelInfo(
                name="svr",
                train_func=self._train_svr,
                params_config=params
            )
        ]

    def train(self, train_df: pd.DataFrame, test_df: pd.DataFrame):
        X_train = train_df[self.config.get("features")]
        y_train = train_df[self.config.get("target")]
        X_test = test_df[self.config.get("features")]
        y_test = test_df[self.config.get("target")]

        scaler = StandardScaler()
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)


        best_score = float("inf")
        best_model_name = None
        best_model = None
        best_metrics: dict[str, float] = {}
        leaderboard: list[dict[str, float | str]] = []

        for model_info in self.models:
            self.logger.info(f"Training {model_info.name} with params: {model_info.params}")
            try:
                model = model_info.train_func(X_train, y_train, X_test, y_test, model_info.params)
                results = evaluate_model(model, X_test, y_test)
                score = results["rmse"]
                leaderboard.append({"model_name": model_info.name, **{k: float(v) for k, v in results.items()}})

                if score < best_score:
                    best_score = score
                    best_model_name = model_info.name
                    best_model = model
                    best_metrics = {k: float(v) for k, v in results.items()}
            except Exception:
                self.logger.error(f"Error training {model_info.name}", exc_info=True)

        if best_model is None or best_model_name is None:
            raise RuntimeError("No model trained successfully")

        save_scaler(scaler)
        save_model(best_model, best_model_name)
        save_metrics(best_metrics, leaderboard)
        self.logger.info("Saved best model locally: model=%s rmse=%.6f", best_model_name, best_score)

    # Training functions for each model type
    def _train_xgboost(self, X_train, y_train, X_test, y_test, params):
        model = xgb.XGBRegressor(**params)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
        return model
    
    def _train_random_forest(self, X_train, y_train, X_test, y_test, params):
        model = RandomForestRegressor(**params)
        model.fit(X_train, y_train)
        return model
    
    def _train_catboost(self, X_train, y_train, X_test, y_test, params):
        model = CatBoostRegressor(**params, allow_writing_files=False)
        model.fit(X_train, y_train, eval_set=(X_test, y_test), use_best_model=True, early_stopping_rounds=50,)
        return model
    
    def _train_lightgbm(self, X_train, y_train, X_test, y_test, params):
        model = lgb.LGBMRegressor(**params)
        model.fit(
            X_train, 
            y_train, 
            eval_set=[(X_test, y_test)], 
            callbacks=[lgb.early_stopping(stopping_rounds=50), lgb.log_evaluation(period=0)]
        )
        return model
    
    def _train_svr(self, X_train, y_train, X_test, y_test, params):
        model = SVR(**params)
        model.fit(X_train, y_train)
        return model
