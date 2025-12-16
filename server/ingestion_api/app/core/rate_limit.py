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

    - Keys are arbitrary strings (we will use: f"{client_id}:{action}")
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

        # lightweight cleanup trigger
        self._ops = 0

    def check(self, key: str) -> RateLimitState:
        """
        Returns RateLimitState with allowed/remaining/reset info.
        """
        now = int(time.time())
        window_start = now - (now % self.window_seconds)
        reset_epoch = window_start + self.window_seconds

        with self._lock:
            self._ops += 1

            prev = self._buckets.get(key)
            if prev is None:
                # first request in this window
                self._buckets[key] = (window_start, 1)
                remaining = self.limit - 1
                return RateLimitState(
                    allowed=True,
                    limit=self.limit,
                    remaining=max(0, remaining),
                    reset_epoch=reset_epoch,
                )

            prev_window_start, count = prev
            if prev_window_start != window_start:
                # new window
                self._buckets[key] = (window_start, 1)
                remaining = self.limit - 1
                return RateLimitState(
                    allowed=True,
                    limit=self.limit,
                    remaining=max(0, remaining),
                    reset_epoch=reset_epoch,
                )

            # same window
            if count >= self.limit:
                return RateLimitState(
                    allowed=False,
                    limit=self.limit,
                    remaining=0,
                    reset_epoch=reset_epoch,
                )

            new_count = count + 1
            self._buckets[key] = (window_start, new_count)
            remaining = self.limit - new_count
            return RateLimitState(
                allowed=True,
                limit=self.limit,
                remaining=max(0, remaining),
                reset_epoch=reset_epoch,
            )

    def maybe_cleanup(self, max_buckets: int = 50_000) -> None:
        """
        Best-effort cleanup to avoid unbounded growth in long-running dev sessions.
        Called optionally from the app after requests.
        """
        # Only attempt cleanup occasionally
        if self._ops % 1000 != 0:
            return

        with self._lock:
            if len(self._buckets) <= max_buckets:
                return

            now = int(time.time())
            cutoff = now - (2 * self.window_seconds)  # keep last ~2 windows
            keys_to_delete = []
            for k, (ws, _) in self._buckets.items():
                if ws < cutoff:
                    keys_to_delete.append(k)

            for k in keys_to_delete:
                self._buckets.pop(k, None)
