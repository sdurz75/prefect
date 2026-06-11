from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from materialize_searches import (
    TERMINAL_STATUSES,
    _keycloak_params,
    authenticate,
    find_and_materialize,
    poll_until_done,
)
from trenolab import ActivationStatus, TaskStatus


# ---------------------------------------------------------------------------
# _keycloak_params
# ---------------------------------------------------------------------------

def test_keycloak_params_extracts_realm_and_base_url():
    url = "https://auth.example.com/realms/myrealm/protocol/openid-connect/token"
    base_url, realm = _keycloak_params(url)
    assert base_url == "https://auth.example.com"
    assert realm == "myrealm"


# ---------------------------------------------------------------------------
# authenticate
# ---------------------------------------------------------------------------

def _config(token_url="https://auth.example.com/realms/testrealm/protocol/openid-connect/token"):
    return {
        "token-url": token_url,
        "client-id": "my-client",
    }


@patch("materialize_searches.KeycloakClient")
def test_authenticate_returns_access_token(mock_kc_cls):
    fake_token = SimpleNamespace(access_token="abc123", expires_in=300)
    mock_kc_cls.return_value.login_client_credentials.return_value = fake_token

    result = authenticate.fn(_config(), "secret")

    assert result == "abc123"
    mock_kc_cls.assert_called_once_with(
        base_url="https://auth.example.com",
        realm="testrealm",
        client_id="my-client",
        client_secret="secret",
    )


# ---------------------------------------------------------------------------
# find_and_materialize
# ---------------------------------------------------------------------------

def _make_search(name, id_="s1"):
    return SimpleNamespace(name=name, id=id_)


def _make_outcome(status, task_id="t42", message="ok"):
    return SimpleNamespace(
        status=status,
        message=message,
        task_handle=SimpleNamespace(id=task_id),
    )


@patch("materialize_searches.TrainSearchesClient")
@patch("materialize_searches._bearer_client")
def test_find_and_materialize_returns_task_id(mock_bearer, mock_ts_cls):
    mock_bearer.return_value.__enter__ = lambda s: MagicMock()
    mock_bearer.return_value.__exit__ = MagicMock(return_value=False)

    ts_instance = mock_ts_cls.return_value
    ts_instance.find_by_scenario.return_value = [
        _make_search("other"),
        _make_search("my-search", id_="s99"),
    ]
    ts_instance.materialize.return_value = _make_outcome(ActivationStatus.ACCEPTED, task_id="t99")

    result = find_and_materialize.fn("http://api", "scen1", "my-search", "user1", "token")

    assert result == "t99"
    ts_instance.materialize.assert_called_once_with("s99", "scen1", user="user1")


@patch("materialize_searches.TrainSearchesClient")
@patch("materialize_searches._bearer_client")
def test_find_and_materialize_raises_when_search_not_found(mock_bearer, mock_ts_cls):
    mock_bearer.return_value.__enter__ = lambda s: MagicMock()
    mock_bearer.return_value.__exit__ = MagicMock(return_value=False)

    mock_ts_cls.return_value.find_by_scenario.return_value = [_make_search("altro")]

    with pytest.raises(ValueError, match="my-search"):
        find_and_materialize.fn("http://api", "scen1", "my-search", "user1", "token")


@patch("materialize_searches.TrainSearchesClient")
@patch("materialize_searches._bearer_client")
def test_find_and_materialize_raises_when_materialize_rejected(mock_bearer, mock_ts_cls):
    mock_bearer.return_value.__enter__ = lambda s: MagicMock()
    mock_bearer.return_value.__exit__ = MagicMock(return_value=False)

    ts_instance = mock_ts_cls.return_value
    ts_instance.find_by_scenario.return_value = [_make_search("my-search")]
    ts_instance.materialize.return_value = _make_outcome(ActivationStatus.REJECTED, message="quota esaurita")

    with pytest.raises(RuntimeError, match="quota esaurita"):
        find_and_materialize.fn("http://api", "scen1", "my-search", "user1", "token")


# ---------------------------------------------------------------------------
# poll_until_done
# ---------------------------------------------------------------------------

def _make_handle(status, progress=100, outcome_message=None):
    return SimpleNamespace(status=status, progress=progress, outcome_message=outcome_message)


@patch("materialize_searches.time.sleep", return_value=None)
@patch("materialize_searches.TasksClient")
@patch("materialize_searches._bearer_client")
def test_poll_until_done_completes_immediately(mock_bearer, mock_tasks_cls, mock_sleep):
    mock_bearer.return_value.__enter__ = lambda s: MagicMock()
    mock_bearer.return_value.__exit__ = MagicMock(return_value=False)

    mock_tasks_cls.return_value.get.return_value = _make_handle(TaskStatus.COMPLETE)

    poll_until_done.fn("http://api", "t1", "token")  # no exception

    mock_sleep.assert_not_called()


@patch("materialize_searches.time.sleep", return_value=None)
@patch("materialize_searches.TasksClient")
@patch("materialize_searches._bearer_client")
def test_poll_until_done_polls_until_complete(mock_bearer, mock_tasks_cls, mock_sleep):
    mock_bearer.return_value.__enter__ = lambda s: MagicMock()
    mock_bearer.return_value.__exit__ = MagicMock(return_value=False)

    mock_tasks_cls.return_value.get.side_effect = [
        _make_handle(TaskStatus.RUNNING, progress=30),
        _make_handle(TaskStatus.RUNNING, progress=70),
        _make_handle(TaskStatus.COMPLETE, progress=100),
    ]

    poll_until_done.fn("http://api", "t1", "token")

    assert mock_sleep.call_count == 2


@patch("materialize_searches.time.sleep", return_value=None)
@patch("materialize_searches.TasksClient")
@patch("materialize_searches._bearer_client")
def test_poll_until_done_raises_on_failure(mock_bearer, mock_tasks_cls, mock_sleep):
    mock_bearer.return_value.__enter__ = lambda s: MagicMock()
    mock_bearer.return_value.__exit__ = MagicMock(return_value=False)

    mock_tasks_cls.return_value.get.return_value = _make_handle(
        TaskStatus.FAILED, outcome_message="errore interno"
    )

    with pytest.raises(RuntimeError, match="errore interno"):
        poll_until_done.fn("http://api", "t1", "token")
