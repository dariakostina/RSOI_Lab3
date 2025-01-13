from datetime import datetime, timedelta
from typing import Any, Callable

from fastapi import HTTPException


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int,
        recovery_timeout_sec: int,
        service_name: str,
        is_critical: bool,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout_sec = recovery_timeout_sec
        self.service_name = service_name
        self.is_critical = is_critical

        self.fail_counter = 0
        self.state_opened_at: datetime | None = None
        self.state = "closed"  # "closed", "open", "half_open"

    def call(self, func: Callable, fallback: Callable | None, *args, **kwargs) -> Any:
        if self.state == "open":
            if (
                self.state_opened_at
                and datetime.now()
                >= self.state_opened_at + timedelta(seconds=self.recovery_timeout_sec)
            ):
                self.state = "half_open"
            else:
                return self._handle_fallback(fallback)

        try:
            result = func(*args, **kwargs)
        except Exception:
            self.fail_counter += 1

            if self.state == "half_open":
                self.state = "open"
                self.state_opened_at = datetime.now()
            else:
                if self.fail_counter >= self.failure_threshold:
                    self.state = "open"
                    self.state_opened_at = datetime.now()

            return self._handle_fallback(fallback)

        self.fail_counter = 0
        self.state = "closed"
        self.state_opened_at = None
        return result

    def _handle_fallback(self, fallback: Callable | None) -> Any:
        if fallback is None:
            if self.is_critical:
                raise HTTPException(
                    status_code=503,
                    detail=f"Service {self.service_name} unavailable",
                )
            else:
                return None

        return fallback()()
