WINDOW_SECONDS = 10


def window_start_for(timestamp: float, window_seconds: int = WINDOW_SECONDS) -> int:
    """Which tumbling window a timestamp falls into, as that window's start time."""
    return (int(timestamp) // window_seconds) * window_seconds
