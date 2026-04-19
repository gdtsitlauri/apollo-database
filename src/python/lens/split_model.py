def predict_split_point(fill_factor: float, skew: float) -> float:
    """Simple placeholder used until the learned model pipeline is added."""
    score = 0.5 + 0.2 * skew - 0.1 * (fill_factor - 0.5)
    return max(0.1, min(0.9, score))
