import time
from dataclasses import dataclass
from threading import Lock
from typing import Dict, Tuple


@dataclass(frozen=True)
class RateLimitState:
    allowed: bool
    limit: int
    remaining: int
    reset_epoch: int  # unix epoch seconds when window resets


class FixedWindowRateLimiter:
    """
    Simple in-memory fixed-window rate limiter.

    - Keys are arbitrary strings (we use f"{client_id}:{action}")
    - Not distributed-safe (OK for dev). Replace with Redis for production.
    """

    def __init__(self, limit: int, window_seconds: int = 60) -> None:
        if limit <= 0:
            raise ValueError("limit must be > 0")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")

        self.limit = limit
        self.window_seconds = window_seconds

        # key -> (window_start_epoch, count)
        self._buckets: Dict[str, Tuple[int, int]] = {}
        self._lock = Lock()
        self._ops = 0

    def check(self, key: str, cost: int = 1) -> RateLimitState:
        """
        Returns RateLimitState with allowed/remaining/reset info.
        cost allows weighting requests (default 1).
        """
        if cost <= 0:
            cost = 1

        now = int(time.time())
        window_start = now - (now % self.window_seconds)
        reset_epoch = window_start + self.window_seconds

        with self._lock:
            self._ops += 1

            prev = self._buckets.get(key)
            if prev is None:
                self._buckets[key] = (window_start, cost)
                remaining = self.limit - cost
                return RateLimitState(True, self.limit, max(0, remaining), reset_epoch)

            prev_window_start, count = prev
            if prev_window_start != window_start:
                self._buckets[key] = (window_start, cost)
                remaining = self.limit - cost
                return RateLimitState(True, self.limit, max(0, remaining), reset_epoch)

            if count + cost > self.limit:
                return RateLimitState(False, self.limit, 0, reset_epoch)

            new_count = count + cost
            self._buckets[key] = (window_start, new_count)
            remaining = self.limit - new_count
            return RateLimitState(True, self.limit, max(0, remaining), reset_epoch)

    def maybe_cleanup(self, max_buckets: int = 50_000) -> None:
        """
        Best-effort cleanup to avoid unbounded growth in long-running dev sessions.
        """
        if self._ops % 1000 != 0:
            return

        with self._lock:
            if len(self._buckets) <= max_buckets:
                return

            now = int(time.time())
            cutoff = now - (2 * self.window_seconds)
            keys_to_delete = [k for k, (ws, _) in self._buckets.items() if ws < cutoff]
            for k in keys_to_delete:
                self._buckets.pop(k, None)
