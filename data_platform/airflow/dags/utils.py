from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

def disable_pipeline_vars(vars: list[str]):
    for var in vars:
        Variable.set(var, "false")

def enable_pipeline_vars(vars: list[str]):
    for var in vars:
        Variable.set(var, "true")

def check_pipeline_var_enabled(var_name: str) -> bool:
    if Variable.get(var_name, default_var="false") != "true":
        raise AirflowSkipException("Model training not enabled yet")
    return True