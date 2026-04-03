from __future__ import annotations

import functools
import random
import time
from typing import Any, Callable, Type, TypeVar

T = TypeVar("T")


def retry(
    exceptions: tuple[Type[BaseException], ...] = (Exception,),
    retries: int = 3,
    backoff_factor: float = 0.5,
    jitter: float = 0.1,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Retry decorator with exponential backoff and jitter."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            attempt = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as err:
                    attempt += 1
                    if attempt > retries:
                        raise
                    sleep_for = backoff_factor * (2 ** (attempt - 1))
                    sleep_for += random.uniform(0, jitter)
                    time.sleep(sleep_for)

        return wrapper

    return decorator
