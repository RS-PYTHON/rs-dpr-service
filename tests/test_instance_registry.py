# Copyright 2023-2026 Airbus, CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the shared instance registry (rs_dpr_service/instance_registry.py)."""

from datetime import timedelta

from rs_dpr_service import instance_registry as registry_module
from rs_dpr_service.instance_registry import (
    list_processor_ids,
    register_instance,
    unregister_instance,
    utc_now,
)


def make_session_mock(mocker, query_result=None):
    """Patch instance_registry.Session so it behaves like a context manager returning a mock session."""
    session = mocker.Mock()
    if query_result is not None:
        session.query.return_value.filter.return_value.all.return_value = query_result
    session_cm = mocker.MagicMock()
    session_cm.__enter__ = mocker.Mock(return_value=session)
    session_cm.__exit__ = mocker.Mock(return_value=False)
    mocker.patch.object(registry_module, "Session", return_value=session_cm)
    return session


def test_register_instance_upserts_and_commits(mocker):
    """register_instance() must execute an upsert statement and commit it."""
    engine = mocker.Mock()
    session = make_session_mock(mocker)

    register_instance(engine, "pod-s1", ["s1_l0", "s1_ard"])

    session.execute.assert_called_once()
    session.commit.assert_called_once()


def test_unregister_instance_deletes_and_commits(mocker):
    """unregister_instance() must delete this instance's row and commit."""
    engine = mocker.Mock()
    session = make_session_mock(mocker)

    unregister_instance(engine, "pod-s1")

    session.query.return_value.filter.return_value.delete.assert_called_once()
    session.commit.assert_called_once()


def test_list_processor_ids_merges_dedupes_and_sorts(mocker):
    """list_processor_ids() must return the sorted union of processor ids from every non-stale row."""
    row_s1 = mocker.Mock(processors="s1_l0, s1_ard")
    row_s3 = mocker.Mock(processors="s3_l0,s3_l1olci,s1_l0")  # s1_l0 duplicated on purpose
    make_session_mock(mocker, query_result=[row_s1, row_s3])

    engine = mocker.Mock()
    result = list_processor_ids(engine, stale_after_seconds=30)

    assert result == ["s1_ard", "s1_l0", "s3_l0", "s3_l1olci"]


def test_list_processor_ids_empty_registry_returns_empty_list(mocker):
    """No registered instance means no processor ids, not an error."""
    make_session_mock(mocker, query_result=[])

    engine = mocker.Mock()
    assert not list_processor_ids(engine, stale_after_seconds=30)


def test_stale_rows_are_excluded_via_the_last_seen_cutoff(mocker):
    """
    Stale rows must be excluded by the SQL WHERE clause itself (last_seen >= cutoff), not filtered in Python.

    This checks that the cutoff bound in the query is close to now() - stale_after_seconds.
    """
    session = make_session_mock(mocker, query_result=[])
    before = utc_now()

    list_processor_ids(mocker.Mock(), stale_after_seconds=30)

    after = utc_now()
    clause = session.query.return_value.filter.call_args[0][0]
    cutoff = clause.right.value

    assert before - timedelta(seconds=30) <= cutoff <= after - timedelta(seconds=30)
