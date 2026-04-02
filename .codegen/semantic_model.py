from __future__ import annotations

"""Executable single-operation semantics for the `lock-set` workload.

The modeled effect for `:add` is the Jepsen-local in-memory write protected by
an etcd lock. Helper failures before that write mean the add did not happen and
must not be reported as success. Cleanup failures after the write do not undo
the effect, so the top-level result remains `:ok`.
"""

import sys
from pathlib import Path


CODEGEN_DIR = Path(__file__).resolve().parent
if str(CODEGEN_DIR) not in sys.path:
    sys.path.insert(0, str(CODEGEN_DIR))

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
    lease_granted = False
    keepalive_started = False
    lock_acquired = False
    result_type = ResultType.FAIL
    error = None

    try:
        ctx.step("grant_lease")
        lease_granted = True

        ctx.step("start_keepalive")
        keepalive_started = True

        ctx.step("acquire_lock")
        lock_acquired = True

        ctx.step("read_shared_collection")
        ctx.step("sleep_inside_critical_section")
        ctx.effect("write_shared_collection")
        result_type = ResultType.OK
    except (DefiniteStepError, IndeterminateStepError) as exc:
        error = exc.step
    finally:
        if lock_acquired:
            ctx.cleanup("release_lock")
        if keepalive_started:
            ctx.cleanup("stop_keepalive")
        if lease_granted:
            ctx.cleanup("revoke_lease")

    return ctx.result(result_type, error=error)


def lock_set_read(ctx: OpContext):
    try:
        ctx.step("read_shared_collection")
        return ctx.result(ResultType.OK)
    except (DefiniteStepError, IndeterminateStepError) as exc:
        return ctx.result(ResultType.FAIL, error=exc.step)


ADD_SCENARIOS = [
    Scenario("all_success"),
    Scenario("grant_lease_definite_failure", {"grant_lease": StepOutcome.DEFINITE_FAILURE}),
    Scenario("grant_lease_indeterminate_failure", {"grant_lease": StepOutcome.INDETERMINATE_FAILURE}),
    Scenario("start_keepalive_definite_failure", {"start_keepalive": StepOutcome.DEFINITE_FAILURE}),
    Scenario("start_keepalive_indeterminate_failure", {"start_keepalive": StepOutcome.INDETERMINATE_FAILURE}),
    Scenario("acquire_lock_definite_failure", {"acquire_lock": StepOutcome.DEFINITE_FAILURE}),
    Scenario("acquire_lock_indeterminate_failure", {"acquire_lock": StepOutcome.INDETERMINATE_FAILURE}),
    Scenario("read_shared_collection_definite_failure", {"read_shared_collection": StepOutcome.DEFINITE_FAILURE}),
    Scenario("read_shared_collection_indeterminate_failure", {"read_shared_collection": StepOutcome.INDETERMINATE_FAILURE}),
    Scenario(
        "sleep_inside_critical_section_definite_failure",
        {"sleep_inside_critical_section": StepOutcome.DEFINITE_FAILURE},
    ),
    Scenario(
        "sleep_inside_critical_section_indeterminate_failure",
        {"sleep_inside_critical_section": StepOutcome.INDETERMINATE_FAILURE},
    ),
    Scenario("write_shared_collection_definite_failure", {"write_shared_collection": StepOutcome.DEFINITE_FAILURE}),
    Scenario(
        "write_shared_collection_indeterminate_failure",
        {"write_shared_collection": StepOutcome.INDETERMINATE_FAILURE},
    ),
    Scenario("release_lock_definite_failure", {"release_lock": StepOutcome.DEFINITE_FAILURE}),
    Scenario("release_lock_indeterminate_failure", {"release_lock": StepOutcome.INDETERMINATE_FAILURE}),
    Scenario("stop_keepalive_definite_failure", {"stop_keepalive": StepOutcome.DEFINITE_FAILURE}),
    Scenario("stop_keepalive_indeterminate_failure", {"stop_keepalive": StepOutcome.INDETERMINATE_FAILURE}),
    Scenario("revoke_lease_definite_failure", {"revoke_lease": StepOutcome.DEFINITE_FAILURE}),
    Scenario("revoke_lease_indeterminate_failure", {"revoke_lease": StepOutcome.INDETERMINATE_FAILURE}),
    Scenario(
        "multiple_cleanup_failures_after_effect",
        {
            "release_lock": StepOutcome.DEFINITE_FAILURE,
            "stop_keepalive": StepOutcome.INDETERMINATE_FAILURE,
            "revoke_lease": StepOutcome.DEFINITE_FAILURE,
        },
    ),
    Scenario(
        "pre_effect_failure_still_runs_cleanup",
        {
            "read_shared_collection": StepOutcome.DEFINITE_FAILURE,
            "release_lock": StepOutcome.DEFINITE_FAILURE,
            "stop_keepalive": StepOutcome.INDETERMINATE_FAILURE,
            "revoke_lease": StepOutcome.DEFINITE_FAILURE,
        },
    ),
]


READ_SCENARIOS = [
    Scenario("read_success"),
    Scenario("read_definite_failure", {"read_shared_collection": StepOutcome.DEFINITE_FAILURE}),
    Scenario("read_indeterminate_failure", {"read_shared_collection": StepOutcome.INDETERMINATE_FAILURE}),
]


def run_self_checks() -> None:
    run_model(lock_set_add, ADD_SCENARIOS, [effect_implies_ok, no_effect_implies_fail, binary_result_only])
    run_model(lock_set_read, READ_SCENARIOS, [binary_result_only])

    assert_result(
        lock_set_add,
        Scenario("successful_write_cleanup_errors", {"release_lock": StepOutcome.DEFINITE_FAILURE}),
        result_type=ResultType.OK,
        effect_happened=True,
        cleanup_errors=("release_lock",),
    )
    assert_result(
        lock_set_add,
        Scenario("lock_timeout_before_write", {"acquire_lock": StepOutcome.INDETERMINATE_FAILURE}),
        result_type=ResultType.FAIL,
        effect_happened=False,
        error="acquire_lock",
    )
    assert_result(
        lock_set_add,
        Scenario(
            "local_write_failed_after_read",
            {"write_shared_collection": StepOutcome.DEFINITE_FAILURE},
        ),
        result_type=ResultType.FAIL,
        effect_happened=False,
        error="write_shared_collection",
    )
    assert_result(
        lock_set_add,
        Scenario(
            "pre_effect_failure_cleanup_recorded",
            {
                "read_shared_collection": StepOutcome.DEFINITE_FAILURE,
                "release_lock": StepOutcome.DEFINITE_FAILURE,
                "stop_keepalive": StepOutcome.INDETERMINATE_FAILURE,
                "revoke_lease": StepOutcome.DEFINITE_FAILURE,
            },
        ),
        result_type=ResultType.FAIL,
        effect_happened=False,
        error="read_shared_collection",
        cleanup_errors=("release_lock", "stop_keepalive", "revoke_lease"),
    )
    assert_result(
        lock_set_read,
        Scenario("read_contract", {"read_shared_collection": StepOutcome.SUCCESS}),
        result_type=ResultType.OK,
        effect_happened=False,
    )


if __name__ == "__main__":
    run_self_checks()
    print("lock-set semantic model self-checks passed")
