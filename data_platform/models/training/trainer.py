import os
import joblib
import optuna
from optuna.pruners import MedianPruner
import numpy as np
import pandas as pd
import xgboost as xgb
import lightgbm as lgb
from catboost import CatBoostRegressor, CatBoostClassifier

from models.evaluation.task_evaluators import evaluate_regression, evaluate_classification
from models.data.splitter import resolve_model_split_config, split_for_model
from models.registry.mlflow_registery import (
    log_dataframe,
    start_run,
    log_params,
    log_metrics,
    log_model,
    register_model,
)

os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")


REGRESSION_MODEL_REGISTRY = {
    "xgboost": xgb.XGBRegressor,
    "lightgbm": lgb.LGBMRegressor,
    "catboost": CatBoostRegressor,
}

CLASSIFICATION_MODEL_REGISTRY = {
    "xgboost": xgb.XGBClassifier,
    "lightgbm": lgb.LGBMClassifier,
    "catboost": CatBoostClassifier,
}


def _metric_direction(metric_name: str) -> str:
    return "minimize" if metric_name in {"rmse", "mae", "logloss"} else "maximize"


class Trainer:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.default_metric = config.get("optimization_metric", "rmse")
        self.interval = config.get("data_interval", "unknown")

    def train(self, df):
        self._validate_schema(df)
        features = self.config["features"]
        self._persist_feature_columns(features)

        tasks = self.config.get("models", {})
        if not tasks:
            legacy_target = self.config.get("target", "log_return_lead1")
            tasks = {legacy_target: {"type": "regression", "optimization_metric": self.default_metric}}

        for model_name, model_cfg in tasks.items():
            split_cfg = resolve_model_split_config(model_cfg, self.config)
            train_df, val_df, test_df, split_meta = split_for_model(df, split_cfg)
            self._log_split_stats(model_name, split_cfg, split_meta)

            task_type = model_cfg.get("type", "regression")
            if task_type == "regression":
                self._train_regression_model(model_name, model_cfg, train_df, val_df, test_df, features)
            elif task_type == "classification":
                self._train_classification_model(model_name, model_cfg, train_df, val_df, test_df, features)
            else:
                raise ValueError(f"Unsupported task type '{task_type}' for task '{model_name}'")

    def _train_regression_model(self, task_name, task_cfg, train_df, val_df, test_df, features):
        self._train_task(
            task_name=task_name,
            task_cfg=task_cfg,
            train_df=train_df,
            val_df=val_df,
            test_df=test_df,
            features=features,
            task_type="regression",
            registry=REGRESSION_MODEL_REGISTRY,
            evaluator=evaluate_regression,
        )

    def _train_classification_model(self, task_name, task_cfg, train_df, val_df, test_df, features):
        self._train_task(
            task_name=task_name,
            task_cfg=task_cfg,
            train_df=train_df,
            val_df=val_df,
            test_df=test_df,
            features=features,
            task_type="classification",
            registry=CLASSIFICATION_MODEL_REGISTRY,
            evaluator=evaluate_classification,
        )

    def _train_task(self, task_name, task_cfg, train_df, val_df, test_df, features, task_type, registry, evaluator):
        assert task_type in ["regression", "classification"]
        if task_name not in train_df.columns:
            raise ValueError(f"Target column '{task_name}' not found")

        train_df = train_df.dropna(subset=[task_name]).copy()
        val_df = val_df.dropna(subset=[task_name]).copy()
        test_df = test_df.dropna(subset=[task_name]).copy()
        self._assert_non_empty_splits(task_name, train_df, val_df, test_df)

        X_train, y_train = train_df[features], train_df[task_name]
        X_val, y_val = val_df[features], val_df[task_name]
        X_test, y_test = test_df[features], test_df[task_name]

        self._validate_training_inputs(X_train, y_train, task_name, task_type)
        self._log_target_debug_stats(y_train, task_name, task_type, "train")
        self._log_target_debug_stats(y_val, task_name, task_type, "val")

        if task_type == "classification":
            y_train = np.asarray(y_train).astype(int)
            y_val = np.asarray(y_val).astype(int)
            y_test = np.asarray(y_test).astype(int)

        metric_name = task_cfg.get("optimization_metric", self.default_metric)
        direction = _metric_direction(metric_name)
        decay_days = task_cfg.get("decay_days", self.config.get("training", {}).get("decay_days", 30))
        weights = self._compute_weights(train_df, decay_days)
        if task_type == "classification":
            weights = self._apply_class_weighting(weights, y_train, task_cfg)

        model_params = self.config.get("model_params", {})
        best_score = float("inf") if direction == "minimize" else -float("inf")
        best_run_id = None
        best_algo_name = None
        best_report = None

        for algo_name, model_cls in registry.items():
            base_params = dict(model_params.get(algo_name, {}))
            if task_type == "classification":
                base_params = self._inject_classification_params(algo_name, base_params, task_cfg)

            run_name = f"{task_name}.{algo_name}"
            self.logger.info("Training task=%s type=%s algo=%s rows=%s", task_name, task_type, algo_name, len(X_train))
            self._log_feature_debug_stats(X_train, task_name, "train")

            try:
                with start_run(run_name=run_name) as run:
                    tuned = self._tune_model(
                        algo_name=algo_name,
                        model_cls=model_cls,
                        task_type=task_type,
                        metric_name=metric_name,
                        direction=direction,
                        base_params=base_params,
                        X_train=X_train,
                        y_train=y_train,
                        X_val=X_val,
                        y_val=y_val,
                        weights=weights,
                        evaluator=evaluator,
                    )
                    full_params = {**base_params, **tuned}

                    model = self._train_model(
                        model_name=algo_name,
                        model_cls=model_cls,
                        X_train=X_train,
                        y_train=y_train,
                        X_val=X_val,
                        y_val=y_val,
                        params=full_params,
                        weights=weights,
                    )
                    y_pred = self._predict_for_task(model, X_test, task_type)
                    metrics = evaluator(y_test, y_pred)
                    self._validate_metrics(task_name, task_type, metrics)
                    if metric_name not in metrics:
                        raise ValueError(f"Optimization metric '{metric_name}' missing for '{task_name}.{algo_name}'")

                    log_params({
                        "task": task_name,
                        "task_type": task_type,
                        "model_name": f"{task_name}.{algo_name}",
                        **full_params,
                    })
                    log_metrics(metrics)

                    report = pd.DataFrame({"y_true": np.asarray(y_test), "y_pred": np.asarray(y_pred)})
                    log_dataframe(report, f"{task_name}.{algo_name}.predictions.csv")
                    if "confusion_matrix" in metrics:
                        cm_raw = np.asarray(metrics["confusion_matrix"])
                        labels = [str(i) for i in range(cm_raw.shape[0])]
                        cm = pd.DataFrame(cm_raw, index=labels, columns=labels)
                        log_dataframe(cm, f"{task_name}.{algo_name}.confusion_matrix.csv")

                    score = metrics[metric_name]
                    is_better = score < best_score if direction == "minimize" else score > best_score
                    if is_better:
                        best_score = score
                        best_run_id = run.info.run_id
                        best_algo_name = algo_name
                        best_report = report

                    log_model(model, f"{task_name}.{algo_name}", "v1")
            except Exception as e:
                self.logger.error("Training failed for %s.%s: %s", task_name, algo_name, e, exc_info=True)
                raise e

        if best_run_id is None:
            raise RuntimeError(f"No successful model for task '{task_name}'")

        with start_run(run_id=best_run_id):
            if best_report is not None:
                log_dataframe(best_report, f"best_{task_name}.{best_algo_name}.predictions.csv", artifact_path="best_model")

        register_model(
            best_run_id,
            f"{task_name}.{best_algo_name}",
            registered_model_name=f"cryptoquant.{task_name}.{self.interval}.{best_algo_name}",
        )

    def _validate_schema(self, df):
        expected = set(self.config.get("expected_columns", []))
        missing = expected - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        features = set(self.config["features"])
        missing_feat = features - set(df.columns)
        if missing_feat:
            raise ValueError(f"Missing features: {missing_feat}")

    def _validate_training_inputs(self, X, y, task_name, task_type):
        if X.isnull().any().any():
            raise ValueError(f"Feature NaNs detected for task '{task_name}'")
        if np.isinf(X.to_numpy(dtype=float)).any():
            raise ValueError(f"Feature inf detected for task '{task_name}'")
        if np.isnan(X.to_numpy(dtype=float)).any():
            raise ValueError(f"Feature NaN detected for task '{task_name}'")
        if y.isnull().any():
            raise ValueError(f"Target NaNs detected for task '{task_name}'")
        if task_type == "classification":
            labels = set(np.asarray(y).astype(int))
            if not labels.issubset({0, 1, 2}):
                raise ValueError(f"Invalid class labels for task '{task_name}': {sorted(labels)}")

    def _validate_metrics(self, task_name, task_type, metrics):
        required = ["rmse", "mae", "correlation"] if task_type == "regression" else ["accuracy", "f1", "precision", "recall"]
        missing = [name for name in required if name not in metrics]
        if missing:
            raise ValueError(f"Missing required metrics for task '{task_name}': {missing}")

    def _log_target_debug_stats(self, y, task_name, task_type, split_name):
        y_np = np.asarray(y)
        self.logger.info(
            "Task=%s split=%s rows=%s target_mean=%.6f target_std=%.6f",
            task_name,
            split_name,
            len(y_np),
            float(np.mean(y_np)),
            float(np.std(y_np)),
        )
        if task_type == "classification":
            unique, counts = np.unique(y_np.astype(int), return_counts=True)
            distribution = {int(label): int(count) for label, count in zip(unique, counts)}
            self.logger.info("Task=%s split=%s class_distribution=%s", task_name, split_name, distribution)

    def _compute_weights(self, df, decay_days):
        age = (df["open_time"].max() - df["open_time"]).dt.total_seconds()
        return np.exp(-age / (60 * 60 * 24 * decay_days))

    def _apply_class_weighting(self, time_weights, y_train, task_cfg):
        if task_cfg.get("class_weighting") != "balanced":
            return time_weights
        labels = np.asarray(y_train)
        unique, counts = np.unique(labels, return_counts=True)
        class_weight_map = {
            label: len(labels) / (len(unique) * count)
            for label, count in zip(unique, counts)
            if count > 0
        }
        class_weights = np.asarray([class_weight_map.get(label, 1.0) for label in labels])
        return time_weights * class_weights

    def _inject_classification_params(self, algo_name, params, task_cfg):
        if task_cfg.get("class_weighting") == "balanced":
            if algo_name == "catboost":
                params.setdefault("auto_class_weights", "Balanced")
            else:
                params.setdefault("class_weight", "balanced")
                if algo_name == "xgboost":
                    params.setdefault("scale_pos_weight", 1.0)
        return params

    def _tune_model(
        self,
        algo_name,
        model_cls,
        task_type,
        metric_name,
        direction,
        base_params,
        X_train,
        y_train,
        X_val,
        y_val,
        weights,
        evaluator,
    ):
        if not self.config.get("tuning", {}).get("enabled", False):
            return {}

        search_space = self.config.get("search_space", {}).get(algo_name, {})
        if not search_space:
            return {}

        def objective(trial):
            try:
                params = self._sample_params(trial, search_space)
                full_params = {**base_params, **params}
                model = self._train_model(
                    model_name=algo_name,
                    model_cls=model_cls,
                    X_train=X_train,
                    y_train=y_train,
                    X_val=X_val,
                    y_val=y_val,
                    params=full_params,
                    weights=weights,
                )
                y_pred = self._predict_for_task(model, X_val, task_type)
                metrics = evaluator(y_val, y_pred)
                if metric_name not in metrics:
                    raise ValueError(f"Tuning metric '{metric_name}' missing for '{algo_name}'")
                return metrics[metric_name]
            except Exception:
                return float("inf") if direction == "minimize" else -float("inf")

        study = optuna.create_study(direction=direction, pruner=MedianPruner())
        study.optimize(objective, n_trials=self.config["tuning"].get("n_trials", 10), timeout=self.config["tuning"].get("timeout_seconds", 600))
        return study.best_params

    def _sample_params(self, trial, space):
        params = {}
        for key, bounds in space.items():
            low, high = bounds
            if isinstance(low, int) and isinstance(high, int):
                params[key] = trial.suggest_int(key, low, high)
            else:
                params[key] = trial.suggest_float(key, low, high, log=True)
        return params

    def _train_model(self, model_name, model_cls, X_train, y_train, X_val, y_val, params, weights):
        if model_name == "catboost":
            model = model_cls(**params, allow_writing_files=False)
            model.fit(
                X_train,
                y_train,
                sample_weight=weights,
                eval_set=(X_val, y_val),
                use_best_model=True,
                early_stopping_rounds=50,
                verbose=False,
            )
            return model

        if model_name == "lightgbm":
            model = model_cls(**params)
            model.fit(
                X_train,
                y_train,
                sample_weight=weights,
                eval_set=[(X_val, y_val)],
                callbacks=[lgb.early_stopping(50), lgb.log_evaluation(period=0)],
            )
            return model

        xgb_params = dict(params)
        xgb_params.setdefault("early_stopping_rounds", 50)
        model = model_cls(**xgb_params)
        model.fit(
            X_train,
            y_train,
            sample_weight=weights,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )
        return model

    def _predict_for_task(self, model, X, task_type):
        if task_type == "classification" and hasattr(model, "predict_proba"):
            probs = model.predict_proba(X)
            if probs.ndim == 2 and probs.shape[1] >= 3:
                classes = getattr(model, "classes_", np.array([0, 1, 2]))
                return np.asarray(classes)[np.argmax(probs, axis=1)].astype(int)
        preds = np.asarray(model.predict(X)).reshape(-1)
        if task_type == "classification":
            return np.clip(np.rint(preds).astype(int), 0, 2)
        return preds.astype(float)

    def _persist_feature_columns(self, features):
        artifact_dir = os.path.join("models", "artifacts")
        os.makedirs(artifact_dir, exist_ok=True)
        joblib.dump(list(features), os.path.join(artifact_dir, "feature_columns.pkl"))

    def _log_feature_debug_stats(self, X, task_name, split_name):
        nan_count = int(X.isna().sum().sum())
        self.logger.info("Task=%s split=%s feature_nan_count=%s", task_name, split_name, nan_count)
        self.logger.info(
            "Task=%s split=%s feature_stats mean=%.6f std=%.6f",
            task_name,
            split_name,
            float(np.nanmean(X.to_numpy(dtype=float))),
            float(np.nanstd(X.to_numpy(dtype=float))),
        )

    def _assert_non_empty_splits(self, task_name, train_df, val_df, test_df):
        min_train_rows = int((self.config.get("training") or {}).get("min_train_rows", 1))
        if len(train_df) < min_train_rows:
            raise ValueError(
                f"Insufficient train rows for '{task_name}': {len(train_df)} < min_train_rows={min_train_rows}"
            )
        if len(val_df) <= 0:
            raise ValueError(f"Validation split empty for '{task_name}'")
        if len(test_df) <= 0:
            raise ValueError(f"Test split empty for '{task_name}'")

    def _log_split_stats(self, model_name, split_cfg, split_meta):
        self.logger.info(
            "Model=%s split_days(train=%s,val=%s,test=%s)",
            model_name,
            split_cfg["train"],
            split_cfg["val"],
            split_cfg["test"],
        )
        self.logger.info(
            "Model=%s ranges train_start=%s val_start=%s test_start=%s max_time=%s",
            model_name,
            split_meta["train_start"],
            split_meta["val_start"],
            split_meta["test_start"],
            split_meta["max_time"],
        )
        self.logger.info(
            "Model=%s samples train=%s val=%s test=%s",
            model_name,
            split_meta["train_rows"],
            split_meta["val_rows"],
            split_meta["test_rows"],
        )
