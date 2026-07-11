"""
Tumbling Windows

Assigns a timestamp to a fixed, non-overlapping 10-second bucket (a
tumbling window) by flooring it to the nearest multiple of
WINDOW_SECONDS.

Chosen over sliding or session windows because the dashboard's metrics
only need to answer "what happened in this fixed slice of time," not
"what happened in the last N seconds as of any instant" (sliding) or
"how long did this customer's activity burst last" (session) --
tumbling is the simplest window that satisfies the actual question
being asked here.
"""

WINDOW_SECONDS = 10


def window_start_for(timestamp: float, window_seconds: int = WINDOW_SECONDS) -> int:
    """Which tumbling window a timestamp falls into, as that window's start time."""
    return (int(timestamp) // window_seconds) * window_seconds
