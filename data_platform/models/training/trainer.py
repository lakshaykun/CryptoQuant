# models/training/trainer.py

from sklearn.preprocessing import StandardScaler
from models.evaluation.evaluate import evaluate_model
import xgboost as xgb
from catboost import CatBoostRegressor
import lightgbm as lgb
import pandas as pd
from models.registry.mlflow_registery import (
    log_dataframe,
    # log_scaler,
    start_run,
    log_params,
    log_metrics,
    log_model,
    register_model,
)

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
                name="catboost",
                train_func=self._train_catboost,
                params_config=params
            ),
            ModelInfo(
                name="lightgbm",
                train_func=self._train_lightgbm,
                params_config=params
            )
        ]

    def train(self, train_df: pd.DataFrame, test_df: pd.DataFrame):
        X_train = train_df[self.config.get("features")]
        y_train = train_df[self.config.get("target")]
        X_test = test_df[self.config.get("features")]
        y_test = test_df[self.config.get("target")]

        # scaler = StandardScaler()
        # X_train = scaler.fit_transform(X_train)
        # X_test = scaler.transform(X_test)


        best_score = float(0)
        best_run_id = None
        best_model_name = None
        best_report = None

        for model_info in self.models:
            self.logger.info(f"Training {model_info.name} with params: {model_info.params}")
            try:
                with start_run(run_name=model_info.name) as run:
                    log_params({
                        "model_name": model_info.name,
                        **model_info.params
                    })

                    model = model_info.train_func(X_train, y_train, X_test, y_test, model_info.params)

                    results, report = evaluate_model(model, X_test, y_test)

                    log_metrics(results)
                    log_dataframe(report, f"{model_info.name}_backtest.csv")

                    score = results["directional_accuracy"]

                    if score > best_score:
                        best_score = score
                        best_run_id = run.info.run_id
                        best_model_name = model_info.name
                        best_report = report

                    log_model(model, model_info.name, "v1")
            except:
                self.logger.error(f"Error training {model_info.name}", exc_info=True)
        
        if best_run_id is None or best_model_name is None:
            raise RuntimeError("No trained model completed successfully, nothing to register in MLflow")

        with start_run(run_id=best_run_id):
            # log_scaler(scaler)

            if best_report is not None:
                log_dataframe(best_report, f"best_{best_model_name}_backtest.csv", artifact_path="best_model")

        register_model(best_run_id, best_model_name)

    # Training functions for each model type
    def _train_xgboost(self, X_train, y_train, X_test, y_test, params):
        model = xgb.XGBRegressor(**params)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
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