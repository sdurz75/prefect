from prefect import flow, task
from prefect.blocks.system import Secret
import httpx

from common.keycloak import get_token


@task(log_prints=True)
async def call_api(endpoint: str, payload: dict | None = None) -> dict:
    config = (await Secret.load("prefect-worker-client-conf")).get()
    token = await get_token()

    response = httpx.request(
        method="POST" if payload else "GET",
        url=f"{config['base-url']}/{endpoint}",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0
    )
    response.raise_for_status()
    return response.json()


@task(log_prints=True)
async def get_user_tasks() -> dict:
    config = (await Secret.load("prefect-worker-client-conf")).get()
    token = await get_token()

    response = httpx.get(
        f"{config['base-url']}/api/tasks/user/{config['client-user']}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0
    )
    response.raise_for_status()
    return response.json()


@flow(name="backend-automation", log_prints=True)
async def automate_operation():
    result = await call_api("v1/some/endpoint", {"key": "value"})
    print(f"Risultato: {result}")
    return result
