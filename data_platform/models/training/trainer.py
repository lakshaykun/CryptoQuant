# models/training/trainer.py

import optuna
import numpy as np
import xgboost as xgb
import lightgbm as lgb
from catboost import CatBoostRegressor

from models.evaluation.evaluate import evaluate_model
from models.registry.mlflow_registery import (
    log_dataframe,
    start_run,
    log_params,
    log_metrics,
    log_model,
    register_model,
)


MODEL_REGISTRY = {
    "xgboost": xgb.XGBRegressor,
    "lightgbm": lgb.LGBMRegressor,
    "catboost": CatBoostRegressor,
}


class ModelInfo:
    def __init__(self, name, params_config):
        self.name = name
        self.base_params = params_config.get(name, {})


class Trainer:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

        self.models = [
            ModelInfo(name, config.get("model_params", {}))
            for name in MODEL_REGISTRY.keys()
        ]

        self.metric_name = config.get("optimization_metric", "rmse")
        self.direction = "minimize" if self.metric_name == "rmse" else "maximize"

    # =========================
    # MAIN TRAIN LOOP
    # =========================
    def train(self, train_df, val_df, test_df):

        self._validate_schema(train_df)

        features = self.config["features"]
        target = self.config["target"]

        X_train, y_train = train_df[features], train_df[target]
        X_val, y_val = val_df[features], val_df[target]
        X_test, y_test = test_df[features], test_df[target]

        weights = self._compute_weights(train_df)

        best_score = float("inf") if self.direction == "minimize" else -float("inf")
        best_run_id = None
        best_model_name = None
        best_report = None

        for model_info in self.models:
            self.logger.info(f"Training {model_info.name}")

            try:
                with start_run(run_name=model_info.name) as run:

                    best_params = self._tune_model(
                        model_info,
                        X_train, y_train,
                        X_val, y_val,
                        weights
                    )

                    full_params = {**model_info.base_params, **best_params}

                    log_params({
                        "model_name": model_info.name,
                        **full_params
                    })

                    model = self._train_model(
                        model_info.name,
                        X_train, y_train,
                        X_val, y_val,
                        full_params,
                        weights
                    )

                    results, report = evaluate_model(model, X_test, y_test)

                    log_metrics(results)
                    log_dataframe(report, f"{model_info.name}_backtest.csv")

                    score = results[self.metric_name]

                    is_better = (
                        score < best_score if self.direction == "minimize"
                        else score > best_score
                    )

                    if is_better:
                        best_score = score
                        best_run_id = run.info.run_id
                        best_model_name = model_info.name
                        best_report = report

                    log_model(model, model_info.name, "v1")

            except Exception:
                self.logger.error(f"Error training {model_info.name}", exc_info=True)

        if best_run_id is None:
            raise RuntimeError("No model trained successfully")

        with start_run(run_id=best_run_id):
            if best_report is not None:
                log_dataframe(
                    best_report,
                    f"best_{best_model_name}_backtest.csv",
                    artifact_path="best_model"
                )

        register_model(best_run_id, best_model_name)

    # =========================
    # SCHEMA VALIDATION
    # =========================
    def _validate_schema(self, df):
        expected = set(self.config.get("expected_columns", []))
        missing = expected - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        features = set(self.config["features"])
        missing_feat = features - set(df.columns)
        if missing_feat:
            raise ValueError(f"Missing features: {missing_feat}")

    # =========================
    # WEIGHTS
    # =========================
    def _compute_weights(self, df):
        decay_days = self.config.get("training", {}).get("decay_days", 30)
        age = (df["open_time"].max() - df["open_time"]).dt.total_seconds()
        return np.exp(-age / (60 * 60 * 24 * decay_days))

    # =========================
    # OPTUNA TUNING
    # =========================
    def _tune_model(self, model_info, X_train, y_train, X_val, y_val, weights):

        if not self.config.get("tuning", {}).get("enabled", False):
            return {}

        search_space = self.config.get("search_space", {}).get(model_info.name, {})

        if not search_space:
            return {}

        def objective(trial):
            params = self._sample_params(trial, search_space)
            full_params = {**model_info.base_params, **params}

            model = self._train_model(
                model_info.name,
                X_train, y_train,
                X_val, y_val,
                full_params,
                weights
            )

            val_results, _ = evaluate_model(model, X_val, y_val)
            return val_results[self.metric_name]

        study = optuna.create_study(direction=self.direction)

        study.optimize(
            objective,
            n_trials=self.config["tuning"].get("n_trials", 20)
        )

        return study.best_params

    # =========================
    # PARAM SAMPLING
    # =========================
    def _sample_params(self, trial, space):
        params = {}
        for key, bounds in space.items():
            low, high = bounds

            if isinstance(low, int) and isinstance(high, int):
                params[key] = trial.suggest_int(key, low, high)
            else:
                params[key] = trial.suggest_float(key, low, high, log=True)

        return params

    # =========================
    # MODEL TRAINING
    # =========================
    def _train_model(self, model_name, X_train, y_train, X_val, y_val, params, weights):

        model_cls = MODEL_REGISTRY[model_name]

        if model_name == "catboost":
            model = model_cls(**params, allow_writing_files=False)
            model.fit(
                X_train,
                y_train,
                sample_weight=weights,
                eval_set=(X_val, y_val),
                use_best_model=True,
                early_stopping_rounds=50,
                verbose=False
            )
        elif model_name == "lightgbm":
            model = model_cls(**params)
            model.fit(
                X_train,
                y_train,
                sample_weight=weights,
                eval_set=[(X_val, y_val)],
                callbacks=[
                    lgb.early_stopping(50),
                    lgb.log_evaluation(period=0)
                ]
            )
        else:  # xgboost
            model = model_cls(**params)
            model.fit(
                X_train,
                y_train,
                sample_weight=weights,
                eval_set=[(X_val, y_val)],
                early_stopping_rounds=50,
                verbose=False
            )

        return model