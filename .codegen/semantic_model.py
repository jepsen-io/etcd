from __future__ import annotations

"""Executable single-operation semantics for the etcd-backed `lock-set`
workload.

The modeled effect is Jepsen-local: overwriting one shared in-memory set after a
stale read. etcd only provides the mutex service around that external state.

Because the effect is local and explicit, top-level `:add` results are binary:

- `ok` if the in-memory write happened
- `fail` if it did not

Cleanup after the write is best-effort only and must not turn an acknowledged
effect into `info` or `fail`.
"""

from operation_semantics import (
    DefiniteStepError,
    IndeterminateStepError,
    OpContext,
    ResultType,
    Scenario,
    StepOutcome,
    assert_result,
    binary_result_only,
    effect_implies_ok,
    no_effect_implies_fail,
    run_model,
)


def lock_set_add(ctx: OpContext):
    result_type = ResultType.OK
    error = None

    try:
        ctx.step("grant_lease")
        ctx.step("start_keepalive")
        ctx.step("acquire_lock")
        ctx.step("read_shared_state")
        ctx.step("sleep_in_critical_section")
        ctx.effect("write_shared_state")
    except (DefiniteStepError, IndeterminateStepError) as exc:
        result_type = ResultType.FAIL
        error = exc.step
    finally:
        ctx.cleanup("release_lock")
        ctx.cleanup("stop_keepalive")
        ctx.cleanup("revoke_lease")

    return ctx.result(result_type, error=error)


def lock_set_read(ctx: OpContext):
    try:
        ctx.step("read_shared_state")
        return ctx.result(ResultType.OK)
    except (DefiniteStepError, IndeterminateStepError) as exc:
        return ctx.result(ResultType.FAIL, error=exc.step)


def read_never_has_effect(result) -> None:
    assert result.effect_happened is False


ADD_SCENARIOS = [
    Scenario("all_success"),
    Scenario("grant_lease_definite_failure", {"grant_lease": StepOutcome.DEFINITE_FAILURE}),
    Scenario(
        "grant_lease_indeterminate_failure",
        {"grant_lease": StepOutcome.INDETERMINATE_FAILURE},
    ),
    Scenario(
        "start_keepalive_definite_failure",
        {"start_keepalive": StepOutcome.DEFINITE_FAILURE},
    ),
    Scenario(
        "start_keepalive_indeterminate_failure",
        {"start_keepalive": StepOutcome.INDETERMINATE_FAILURE},
    ),
    Scenario("acquire_lock_definite_failure", {"acquire_lock": StepOutcome.DEFINITE_FAILURE}),
    Scenario(
        "acquire_lock_indeterminate_failure",
        {"acquire_lock": StepOutcome.INDETERMINATE_FAILURE},
    ),
    Scenario(
        "read_shared_state_failure",
        {"read_shared_state": StepOutcome.DEFINITE_FAILURE},
    ),
    Scenario(
        "sleep_in_critical_section_failure",
        {"sleep_in_critical_section": StepOutcome.DEFINITE_FAILURE},
    ),
    Scenario(
        "write_shared_state_failure",
        {"write_shared_state": StepOutcome.DEFINITE_FAILURE},
    ),
    Scenario(
        "release_lock_cleanup_failure",
        {"release_lock": StepOutcome.INDETERMINATE_FAILURE},
    ),
    Scenario(
        "stop_keepalive_cleanup_failure",
        {"stop_keepalive": StepOutcome.DEFINITE_FAILURE},
    ),
    Scenario(
        "revoke_lease_cleanup_failure",
        {"revoke_lease": StepOutcome.INDETERMINATE_FAILURE},
    ),
    Scenario(
        "all_cleanup_steps_fail_after_effect",
        {
            "release_lock": StepOutcome.DEFINITE_FAILURE,
            "stop_keepalive": StepOutcome.INDETERMINATE_FAILURE,
            "revoke_lease": StepOutcome.DEFINITE_FAILURE,
        },
    ),
]


READ_SCENARIOS = [
    Scenario("read_success"),
    Scenario("read_failure", {"read_shared_state": StepOutcome.DEFINITE_FAILURE}),
]


def run_self_checks() -> None:
    run_model(
        lock_set_add,
        ADD_SCENARIOS,
        [binary_result_only, effect_implies_ok, no_effect_implies_fail],
    )
    run_model(
        lock_set_read,
        READ_SCENARIOS,
        [binary_result_only, read_never_has_effect],
    )

    assert_result(
        lock_set_add,
        Scenario("pre_effect_indeterminate_lock_failure", {"acquire_lock": StepOutcome.INDETERMINATE_FAILURE}),
        result_type=ResultType.FAIL,
        effect_happened=False,
        error="acquire_lock",
        cleanup_errors=(),
    )
    assert_result(
        lock_set_add,
        Scenario(
            "cleanup_failures_do_not_change_ok",
            {
                "release_lock": StepOutcome.DEFINITE_FAILURE,
                "stop_keepalive": StepOutcome.INDETERMINATE_FAILURE,
                "revoke_lease": StepOutcome.DEFINITE_FAILURE,
            },
        ),
        result_type=ResultType.OK,
        effect_happened=True,
        cleanup_errors=("release_lock", "stop_keepalive", "revoke_lease"),
    )
    assert_result(
        lock_set_read,
        Scenario("read_contract", {}),
        result_type=ResultType.OK,
        effect_happened=False,
    )


if __name__ == "__main__":
    run_self_checks()
    print("lock_set semantic model self-checks passed")
