def _is_float(x: str | float | int) -> bool:
    try:
        float(x)
        return True
    except (ValueError, TypeError):
        return False
