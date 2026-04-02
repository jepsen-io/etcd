from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Mapping


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
        self.step = step
        super().__init__(step)


class DefiniteStepError(StepError):
    pass


class IndeterminateStepError(StepError):
    pass


@dataclass(frozen=True)
class Scenario:
    name: str
    outcomes: Mapping[str, StepOutcome | bool] = field(default_factory=dict)

    def outcome_for(self, step: str) -> StepOutcome:
        outcome = self.outcomes.get(step, StepOutcome.SUCCESS)
        if isinstance(outcome, bool):
            return StepOutcome.DEFINITE_FAILURE if outcome else StepOutcome.SUCCESS
        return outcome


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
        self.cleanup_errors: list[str] = []

    def step(self, name: str) -> None:
        outcome = self.scenario.outcome_for(name)
        if outcome is StepOutcome.SUCCESS:
            return
        if outcome is StepOutcome.DEFINITE_FAILURE:
            raise DefiniteStepError(name)
        if outcome is StepOutcome.INDETERMINATE_FAILURE:
            raise IndeterminateStepError(name)
        raise AssertionError(f"unexpected step outcome: {outcome}")

    def effect(self, name: str) -> None:
        self.step(name)
        self.effect_happened = True

    def cleanup(self, name: str) -> None:
        try:
            self.step(name)
        except StepError as exc:
            self.cleanup_errors.append(exc.step)

    def result(self, result_type: ResultType, error: str | None = None) -> OpResult:
        return OpResult(
            result_type=result_type,
            effect_happened=self.effect_happened,
            error=error,
            cleanup_errors=tuple(self.cleanup_errors),
        )


Operation = Callable[[OpContext], OpResult]
Invariant = Callable[[OpResult], None]


def run_operation(operation: Operation, scenario: Scenario) -> OpResult:
    return operation(OpContext(scenario))


def run_model(operation: Operation, scenarios: list[Scenario], invariants: list[Invariant]) -> None:
    for scenario in scenarios:
        result = run_operation(operation, scenario)
        for invariant in invariants:
            try:
                invariant(result)
            except AssertionError as exc:
                invariant_name = getattr(invariant, "__name__", repr(invariant))
                message = str(exc) or "assertion failed"
                raise AssertionError(f"scenario={scenario.name} invariant={invariant_name}: {message}") from exc


def assert_result(
    operation: Operation,
    scenario: Scenario,
    *,
    result_type: ResultType,
    effect_happened: bool,
    error: str | None = None,
    cleanup_errors: tuple[str, ...] | None = None,
) -> OpResult:
    result = run_operation(operation, scenario)
    assert result.result_type is result_type
    assert result.effect_happened is effect_happened
    if error is not None:
        assert result.error == error
    if cleanup_errors is not None:
        assert result.cleanup_errors == cleanup_errors
    return result


def effect_implies_ok(result: OpResult) -> None:
    if result.effect_happened:
        assert result.result_type is ResultType.OK


def no_effect_implies_fail(result: OpResult) -> None:
    if not result.effect_happened:
        assert result.result_type is ResultType.FAIL


def info_forbidden(result: OpResult) -> None:
    assert result.result_type is not ResultType.INFO


def info_implies_no_effect(result: OpResult) -> None:
    if result.result_type is ResultType.INFO:
        assert result.effect_happened is False


def binary_result_only(result: OpResult) -> None:
    assert result.result_type in {ResultType.OK, ResultType.FAIL}
