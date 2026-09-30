"""Per-event attempt guard for Manipulation x25."""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AttemptDecision:
    allowed: bool
    state: str
    reason: str
    attempts: int


class ManipulationAttemptGuard:
    MAX_ATTEMPTS = 2

    def __init__(self):
        self._attempts = {}
        self._locked = set()

    def status(self, event_id):
        attempts=self._attempts.get(event_id,0)
        locked=event_id in self._locked or attempts >= self.MAX_ATTEMPTS
        return AttemptDecision(not locked, "EVENT_LOCKED" if locked else "READY",
                               "max_attempts_reached" if locked else "ok", attempts)

    def register_attempt(self, event_id, *, new_trigger):
        current=self.status(event_id)
        if not current.allowed:
            return current
        if not new_trigger:
            return AttemptDecision(False, "WAIT_NEW_TRIGGER", "new_structure_trigger_required", current.attempts)
        attempts=current.attempts+1
        self._attempts[event_id]=attempts
        if attempts >= self.MAX_ATTEMPTS:
            # The second attempt itself is allowed; subsequent calls are locked.
            return AttemptDecision(True, "LAST_ATTEMPT", "second_and_final_attempt", attempts)
        return AttemptDecision(True, "ATTEMPT_ALLOWED", "ok", attempts)

    def reset_for_new_event(self, event_id):
        self._attempts.pop(event_id,None)
        self._locked.discard(event_id)
