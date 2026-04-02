from __future__ import annotations

"""Executable semantic model for the lock-set workload's top-level :add op.

The modeled effect is the Jepsen-local write to shared in-memory state.
Failures before that write mean the add did not happen, even if an etcd helper
failed in an indeterminate way. Failures after that write are cleanup failures
and do not change the top-level result away from OK.
"""

from dataclasses import dataclass, field
from enum import Enum


class ResultType(str, Enum):
    OK = "ok"
    FAIL = "fail"
    INFO = "info"


class StepOutcome(str, Enum):
    SUCCESS = "success"
    DEFINITE_FAILURE = "definite_failure"
    INDETERMINATE_FAILURE = "indeterminate_failure"


class StepError(RuntimeError):
    def __init__(self, step: str):
        super().__init__(step)
        self.step = step


class DefiniteStepError(StepError):
    pass


class IndeterminateStepError(StepError):
    pass


@dataclass(frozen=True)
class Scenario:
    name: str
    outcomes: dict[str, StepOutcome] = field(default_factory=dict)


@dataclass(frozen=True)
class OpResult:
    result_type: ResultType
    effect_happened: bool
    error: str | None = None
    cleanup_errors: tuple[str, ...] = ()


class OpContext:
    def __init__(self, scenario: Scenario):
        self.scenario = scenario
        self.effect_happened = False

    def _run(self, step: str, *, effect: bool) -> None:
        outcome = self.scenario.outcomes.get(step, StepOutcome.SUCCESS)
        if outcome is StepOutcome.SUCCESS:
            if effect:
                self.effect_happened = True
            return
        if outcome is StepOutcome.DEFINITE_FAILURE:
            raise DefiniteStepError(step)
        if outcome is StepOutcome.INDETERMINATE_FAILURE:
            raise IndeterminateStepError(step)
        raise AssertionError(f"unknown step outcome: {outcome}")

    def step(self, step: str) -> None:
        self._run(step, effect=False)

    def effect(self, step: str) -> None:
        self._run(step, effect=True)

    def result(
        self,
        result_type: ResultType,
        *,
        error: str | None = None,
        cleanup_errors: tuple[str, ...] = (),
    ) -> OpResult:
        return OpResult(
            result_type=result_type,
            effect_happened=self.effect_happened,
            error=error,
            cleanup_errors=cleanup_errors,
        )


def lock_set_add(ctx: OpContext) -> OpResult:
    try:
        ctx.step("grant_lease")
        ctx.step("start_keepalive")
        ctx.step("acquire_lock")
        ctx.step("read_shared_state")
        ctx.step("sleep_inside_critical_section")
        ctx.effect("write_shared_state")
    except (DefiniteStepError, IndeterminateStepError) as exc:
        return ctx.result(ResultType.FAIL, error=exc.step)

    cleanup_errors: list[str] = []
    for step in ("release_lock", "stop_keepalive", "revoke_lease"):
        try:
            ctx.step(step)
        except (DefiniteStepError, IndeterminateStepError) as exc:
            cleanup_errors.append(exc.step)

    return ctx.result(ResultType.OK, cleanup_errors=tuple(cleanup_errors))


def assert_result(
    scenario: Scenario,
    *,
    result_type: ResultType,
    effect_happened: bool,
    error: str | None = None,
    cleanup_errors: tuple[str, ...] = (),
) -> None:
    actual = lock_set_add(OpContext(scenario))
    expected = OpResult(
        result_type=result_type,
        effect_happened=effect_happened,
        error=error,
        cleanup_errors=cleanup_errors,
    )
    if actual != expected:
        raise AssertionError(f"{scenario.name}: expected {expected}, got {actual}")


def run_self_checks() -> None:
    assert_result(
        Scenario("all_success"),
        result_type=ResultType.OK,
        effect_happened=True,
    )
    assert_result(
        Scenario(
            "grant_lease_times_out",
            {"grant_lease": StepOutcome.INDETERMINATE_FAILURE},
        ),
        result_type=ResultType.FAIL,
        effect_happened=False,
        error="grant_lease",
    )
    assert_result(
        Scenario(
            "lock_session_expires_before_effect",
            {"acquire_lock": StepOutcome.INDETERMINATE_FAILURE},
        ),
        result_type=ResultType.FAIL,
        effect_happened=False,
        error="acquire_lock",
    )
    assert_result(
        Scenario(
            "sleep_fails_before_write",
            {"sleep_inside_critical_section": StepOutcome.DEFINITE_FAILURE},
        ),
        result_type=ResultType.FAIL,
        effect_happened=False,
        error="sleep_inside_critical_section",
    )
    assert_result(
        Scenario(
            "write_fails",
            {"write_shared_state": StepOutcome.DEFINITE_FAILURE},
        ),
        result_type=ResultType.FAIL,
        effect_happened=False,
        error="write_shared_state",
    )
    assert_result(
        Scenario(
            "unlock_fails_after_effect",
            {"release_lock": StepOutcome.DEFINITE_FAILURE},
        ),
        result_type=ResultType.OK,
        effect_happened=True,
        cleanup_errors=("release_lock",),
    )
    assert_result(
        Scenario(
            "revoke_times_out_after_effect",
            {"revoke_lease": StepOutcome.INDETERMINATE_FAILURE},
        ),
        result_type=ResultType.OK,
        effect_happened=True,
        cleanup_errors=("revoke_lease",),
    )


if __name__ == "__main__":
    run_self_checks()
    print("lock_set_semantics.py self-checks passed")
