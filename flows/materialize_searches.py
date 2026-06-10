from prefect import flow, task
from prefect.blocks.system import Secret, JSON
from datetime import timedelta
import httpx

from common import keycloak

@task(log_prints=True)
def call_api(endpoint: str, payload: dict | None = None) -> dict:
    config = JSON.load("backend-config").value
    token = keycloak.get_keycloak_token()

    response = httpx.request(
        method="POST" if payload else "GET",
        url=f"{config['base_url']}/{endpoint}",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0
    )
    response.raise_for_status()
    return response.json()

@flow(name="backend-automation", log_prints=True)
def automate_operation():
    # chiama il tuo backend
    result = call_api("v1/some/endpoint", {"key": "value"})
    print(f"Risultato: {result}")
    return result

