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

"""Tests for rs_dpr_service.dask.dask_cluster_handler."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from opentelemetry.trace import StatusCode

from rs_dpr_service.dask.call_dask import ClusterInfo
from rs_dpr_service.dask.dask_cluster_handler import DaskClusterHandler
from rs_dpr_service.utils.settings import set_dask_env


def _mock_nominal_gateway_connection(mocker, monkeypatch, scheduler_info):
    """Prepare a mocked Dask Gateway connection for setup_dask_connection()."""
    # The handler reads these env vars while building the Gateway connection.
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")
    monkeypatch.setenv("DASK_GATEWAY__AUTH__TYPE", "jupyterhub")

    span = mocker.Mock()

    @contextmanager
    def fake_start_span(*args, **kwargs):
        """Yield a fake tracing span around setup_dask_connection()."""
        assert args == ("rs_dpr_service.dask.dask_cluster_handler", "setup_dask_connection")
        assert not kwargs
        yield span

    cluster_info = ClusterInfo(jupyter_token="token", cluster_label="dask-l0")  # nosec B106
    handler = DaskClusterHandler(cluster_info=cluster_info, local_mode_address="")

    # The newest matching cluster should be selected after sorting by start_time.
    gateway_cluster = mocker.Mock()
    gateway_cluster.dashboard_link = "http://dashboard.test"
    listed_cluster = SimpleNamespace(
        name="cluster-instance",
        start_time=2,
        options={"cluster_name": "dask-l0"},
    )
    older_cluster = SimpleNamespace(
        name="older-cluster-instance",
        start_time=1,
        options={"cluster_name": "dask-l0"},
    )

    gateway = mocker.Mock()
    gateway.list_clusters.return_value = [older_cluster, listed_cluster]
    gateway.connect.return_value = gateway_cluster
    gateway_cls = mocker.patch("rs_dpr_service.dask.dask_cluster_handler.Gateway", return_value=gateway)

    # scheduler_info can be either a successful payload or an exception, depending on the test.
    dask_client = mocker.Mock()
    dask_client.scheduler_info.side_effect = scheduler_info if isinstance(scheduler_info, Exception) else None
    dask_client.scheduler_info.return_value = None if isinstance(scheduler_info, Exception) else scheduler_info
    client_cls = mocker.patch("rs_dpr_service.dask.dask_cluster_handler.Client", return_value=dask_client)
    upload_this_module = mocker.patch("rs_dpr_service.dask.dask_cluster_handler.call_dask.upload_this_module")
    mocker.patch("rs_dpr_service.dask.dask_cluster_handler.start_span", side_effect=fake_start_span)

    return {
        "client_cls": client_cls,
        "dask_client": dask_client,
        "gateway_cls": gateway_cls,
        "gateway": gateway,
        "gateway_cluster": gateway_cluster,
        "handler": handler,
        "span": span,
        "upload_this_module": upload_this_module,
    }


def test_setup_dask_connection_connects_to_matching_gateway_cluster(mocker, monkeypatch):
    """Test setup_dask_connection() connects to the matching Dask Gateway cluster."""
    context = _mock_nominal_gateway_connection(
        mocker,
        monkeypatch,
        scheduler_info={"workers": {"worker-1": {}}},
    )

    handler = context["handler"]
    gateway = context["gateway"]
    gateway_cls = context["gateway_cls"]
    gateway_cluster = context["gateway_cluster"]
    client_cls = context["client_cls"]
    dask_client = context["dask_client"]
    span = context["span"]

    result = handler.setup_dask_connection()

    # Gateway lookup: create the Gateway and connect to the newest cluster matching the requested label.
    gateway_cls.assert_called_once()
    assert gateway_cls.call_args.kwargs["address"] == "http://dask-gateway.test"
    gateway.connect.assert_called_once_with("cluster-instance")

    # Client setup: wrap the connected GatewayCluster and forward worker logs to the service.
    client_cls.assert_called_once_with(gateway_cluster)
    # dask_client.forward_logging.assert_called_once_with()

    # Worker preparation: upload this service code and propagate required environment values.
    context["upload_this_module"].assert_called_once_with(dask_client)
    dask_client.run.assert_called_once_with(set_dask_env, mocker.ANY)

    # Worker check: existing workers mean the temporary scale-up branch is not used.
    dask_client.get_versions.assert_called_once_with(check=True)
    dask_client.scheduler_info.assert_called_once_with()
    gateway_cluster.scale.assert_not_called()

    # Successful setup updates the trace span and stores the connected cluster on the handler.
    span.set_status.assert_called_once_with(StatusCode.OK, str(dask_client))
    assert handler.cluster is gateway_cluster
    assert handler.cluster_info.cluster_instance == "cluster-instance"
    assert result is dask_client


def test_setup_dask_connection_scales_cluster_when_no_workers_are_running(mocker, monkeypatch):
    """Test setup_dask_connection() scales the cluster when scheduler reports no workers."""
    context = _mock_nominal_gateway_connection(mocker, monkeypatch, scheduler_info={"workers": {}})

    result = context["handler"].setup_dask_connection()

    # Empty worker map triggers the temporary scale-up branch.
    context["gateway_cluster"].scale.assert_called_once_with(1)
    context["span"].set_status.assert_called_once_with(StatusCode.OK, str(context["dask_client"]))
    assert result is context["dask_client"]


def test_setup_dask_connection_records_error_when_scheduler_info_fails(mocker, monkeypatch):
    """Test setup_dask_connection() records an error when scheduler_info() fails."""
    context = _mock_nominal_gateway_connection(
        mocker,
        monkeypatch,
        scheduler_info=RuntimeError("Cannot read scheduler info"),
    )
    record_error = mocker.patch("rs_dpr_service.dask.dask_cluster_handler.record_error")

    with pytest.raises(RuntimeError, match="Dask cluster client failed"):
        context["handler"].setup_dask_connection()

    # The inner scheduler error and the outer setup error both record the same failure.
    context["dask_client"].scheduler_info.assert_called_once_with()
    context["gateway_cluster"].scale.assert_not_called()
    assert record_error.call_count == 2


@pytest.mark.parametrize(
    ("case_name", "auth_type", "clusters", "connect_result", "expected_message"),
    [
        ("unsupported_auth", "unsupported", [], object(), "Unsupported authentication type"),
        ("missing_cluster", "jupyterhub", [], object(), "No dask cluster named"),
        ("connect_returns_none", "jupyterhub", ["dask-l0"], None, "Failed to create the cluster"),
    ],
)
def test_setup_dask_connection_raises_for_gateway_connection_errors(
    mocker,
    monkeypatch,
    case_name,
    auth_type,
    clusters,
    connect_result,
    expected_message,
):
    """Test setup_dask_connection() raises clear errors for Dask Gateway connection failures."""
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")
    monkeypatch.setenv("DASK_GATEWAY__AUTH__TYPE", auth_type)

    span = mocker.Mock()

    @contextmanager
    def fake_start_span(*args, **kwargs):
        """Yield a fake tracing span around setup_dask_connection()."""
        assert args == ("rs_dpr_service.dask.dask_cluster_handler", "setup_dask_connection")
        assert not kwargs
        yield span

    gateway = mocker.Mock()
    gateway.list_clusters.return_value = [
        SimpleNamespace(name=f"{label}-instance", start_time=index, options={"cluster_name": label})
        for index, label in enumerate(clusters)
    ]
    gateway.connect.return_value = connect_result
    gateway_cls = mocker.patch("rs_dpr_service.dask.dask_cluster_handler.Gateway", return_value=gateway)
    client_cls = mocker.patch("rs_dpr_service.dask.dask_cluster_handler.Client")
    record_error = mocker.patch("rs_dpr_service.dask.dask_cluster_handler.record_error")
    mocker.patch("rs_dpr_service.dask.dask_cluster_handler.start_span", side_effect=fake_start_span)

    handler = DaskClusterHandler(
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
        local_mode_address="",
    )

    with pytest.raises(RuntimeError, match=expected_message):
        handler.setup_dask_connection()

    # Unsupported auth fails before Gateway is created; cluster lookup failures happen after Gateway creation.
    if case_name == "unsupported_auth":
        gateway_cls.assert_not_called()
        gateway.list_clusters.assert_not_called()
    else:
        gateway_cls.assert_called_once()
        gateway.list_clusters.assert_called_once_with()

    # None of these gateway failures should reach Dask Client creation.
    client_cls.assert_not_called()
    record_error.assert_called_once_with(span, mocker.ANY)


def test_setup_dask_connection_raises_when_gateway_auth_env_is_missing(mocker, monkeypatch):
    """Test setup_dask_connection() raises when Dask Gateway auth env is missing."""
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")
    monkeypatch.delenv("DASK_GATEWAY__AUTH__TYPE", raising=False)

    span = mocker.Mock()

    @contextmanager
    def fake_start_span(*args, **kwargs):
        """Yield a fake tracing span around setup_dask_connection()."""
        assert args == ("rs_dpr_service.dask.dask_cluster_handler", "setup_dask_connection")
        assert not kwargs
        yield span

    gateway_cls = mocker.patch("rs_dpr_service.dask.dask_cluster_handler.Gateway")
    client_cls = mocker.patch("rs_dpr_service.dask.dask_cluster_handler.Client")
    record_error = mocker.patch("rs_dpr_service.dask.dask_cluster_handler.record_error")
    mocker.patch("rs_dpr_service.dask.dask_cluster_handler.start_span", side_effect=fake_start_span)

    handler = DaskClusterHandler(
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
        local_mode_address="",
    )

    with pytest.raises(RuntimeError, match="Missing key"):
        handler.setup_dask_connection()

    # Missing auth configuration fails before the Gateway or Dask Client can be created.
    gateway_cls.assert_not_called()
    client_cls.assert_not_called()
    record_error.assert_called_once_with(span, mocker.ANY)
