from utils.number_utils import to_float


def normalized_weights(raw_weights: dict, defaults: dict[str, float]) -> dict[str, float]:
    parsed = {
        key: max(0.0, to_float(raw_weights.get(key), default_value))
        for key, default_value in defaults.items()
    }

    total = sum(parsed.values())
    if total <= 0:
        return defaults.copy()

    return {key: value / total for key, value in parsed.items()}
