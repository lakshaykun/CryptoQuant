class InferencePipeline:
    def __init__(self, model, model_config):
        self.model = model
        self.model_config = model_config

    def run(self, df):
        X = df[self.model_config.get("features")]
        if X.empty:
            raise ValueError("No engineered feature rows provided")
        return self.model.predict(X)
