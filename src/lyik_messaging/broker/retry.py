"""Retry policy helpers for broker consumers."""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """
    Simple exponential-backoff retry policy.
    
    Attributes:
        `max_retries`: The maximum number of retry attempts before giving up.
        `base_delay_ms`: The base delay in milliseconds for the first retry attempt, which will be multiplied exponentially 
                        for subsequent attempts.
        `max_delay_ms`: Optional maximum delay in milliseconds to cap the exponential backoff, preventing excessively long delays.
        `jitter`: Whether to apply random jitter to the delay to help prevent thundering herd problems when multiple messages 
                are retried at the same time.
    """ 

    max_retries: int
    base_delay_ms: int = 100
    max_delay_ms: int | None = None
    jitter: bool = False

    def compute_delay_seconds(self, retry_index: int) -> float:
        """Return sleep delay for a zero-based retry index."""

        delay_ms = self.base_delay_ms * (2**retry_index)
        if self.max_delay_ms is not None:
            delay_ms = min(delay_ms, self.max_delay_ms)
        if self.jitter:
            delay_ms = random.uniform(0.0, float(delay_ms))
        return float(delay_ms) / 1000.0
