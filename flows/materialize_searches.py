"""
Prefect resources richieste:
  - Variable "prefect-worker-conf" (JSON) con chiavi:
        base-url      → base URL di tl-boar
        client-id     → OAuth client ID
        token-url     → URL completo del token endpoint Keycloak
        client-user   → username da passare alla materialize
  - Secret "prefect-worker-client-secret" → client secret OAuth (stringa)
"""

import time
from urllib.parse import urlparse

import httpx
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.variables import Variable

from trenolab import (
    ActivationStatus,
    KeycloakClient,
    TaskStatus,
    TasksClient,
    TrainSearchesClient,
)

SEARCH_NAME = "Test-Materialize-PY"
POLL_INTERVAL_SECONDS = 10
TERMINAL_STATUSES = {TaskStatus.COMPLETE, TaskStatus.FAILED, TaskStatus.CANCELED}


def _keycloak_params(token_url: str) -> tuple[str, str]:
    parsed = urlparse(token_url)
    parts = parsed.path.split("/")
    realm = parts[2]
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    return base_url, realm


def _bearer_client(token: str) -> httpx.Client:
    return httpx.Client(headers={"Authorization": f"Bearer {token}"}, timeout=30.0)


@task(log_prints=True)
def authenticate(config: dict, client_secret: str) -> str:
    base_url, realm = _keycloak_params(config["token-url"])
    kc = KeycloakClient(
        base_url=base_url,
        realm=realm,
        client_id=config["client-id"],
        client_secret=client_secret,
    )
    token = kc.login_client_credentials()
    print(f"Token ottenuto, scade tra {token.expires_in}s")
    return token.access_token


@task(log_prints=True)
def find_and_materialize(base_url: str, scenario_id: str, user: str, token: str) -> str:
    with _bearer_client(token) as client:
        ts_client = TrainSearchesClient(base_url, client)

        searches = ts_client.find_by_scenario(scenario_id)
        train_search = next((s for s in searches if s.name == SEARCH_NAME), None)
        if train_search is None:
            raise ValueError(
                f"TrainSearch '{SEARCH_NAME}' non trovata nello scenario {scenario_id}"
            )
        print(f"TrainSearch trovata: '{train_search.name}' (id={train_search.id})")

        outcome = ts_client.materialize(train_search.id, scenario_id, user=user)
        if outcome.status != ActivationStatus.ACCEPTED:
            raise RuntimeError(f"Materialize rifiutata: {outcome.message}")

        task_id = outcome.task_handle.id
        print(f"Task avviata con id={task_id} — {outcome.message}")
        return task_id


@task(log_prints=True)
def poll_until_done(base_url: str, task_id: str, token: str) -> None:
    with _bearer_client(token) as client:
        tasks_client = TasksClient(base_url, client)
        handle = None
        while True:
            handle = tasks_client.get(task_id)
            print(f"[{handle.status}] progresso={handle.progress}%")
            if handle.status in TERMINAL_STATUSES:
                break
            time.sleep(POLL_INTERVAL_SECONDS)

    if handle.status == TaskStatus.COMPLETE:
        print(f"SUCCESSO — {handle.outcome_message or 'completato'}")
    else:
        raise RuntimeError(
            f"Task terminata con status={handle.status}: {handle.outcome_message or ''}"
        )


@flow(name="materialize-test-search", log_prints=True)
async def materialize_test_search(scenario_id: str):
    config = await Variable.get("prefect-worker-conf")
    client_secret = (await Secret.load("prefect-worker-client-secret")).get()

    token = authenticate(config, client_secret)
    task_id = find_and_materialize(
        config["base-url"],
        scenario_id,
        config["client-user"],
        token,
    )
    poll_until_done(config["base-url"], task_id, token)


if __name__ == "__main__":
    import asyncio
    asyncio.run(materialize_test_search(scenario_id="9GmWq0crK1XrTFBE"))
