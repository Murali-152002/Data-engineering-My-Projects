"""Tests for the retry/backoff and alerting utilities in pipeline_utils.py."""

import pytest

from pipeline_utils import retry_on_failure


def test_retry_succeeds_after_transient_failures():
    """A function that fails twice then succeeds should still return
    the correct result - retries should be transparent to the caller."""
    attempts = {"count": 0}

    @retry_on_failure(max_retries=3, backoff_seconds=0)
    def flaky():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConnectionError("transient failure")
        return "ok"

    assert flaky() == "ok"
    assert attempts["count"] == 3


def test_retry_raises_after_exhausting_attempts():
    """If every attempt fails, the original exception should propagate
    (not be swallowed) so the caller can alert and fail the run."""
    attempts = {"count": 0}

    @retry_on_failure(max_retries=2, backoff_seconds=0)
    def always_fails():
        attempts["count"] += 1
        raise ValueError("permanent failure")

    with pytest.raises(ValueError, match="permanent failure"):
        always_fails()
    assert attempts["count"] == 2


def test_retry_does_not_retry_on_first_success():
    """A function that succeeds immediately should only be called once."""
    attempts = {"count": 0}

    @retry_on_failure(max_retries=3, backoff_seconds=0)
    def works_first_try():
        attempts["count"] += 1
        return "done"

    assert works_first_try() == "done"
    assert attempts["count"] == 1
