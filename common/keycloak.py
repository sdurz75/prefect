# common/keycloak.py
from prefect import task
from prefect.blocks.system import Secret
from datetime import timedelta
import httpx

@task(cache_key_fn=lambda *args, **kwargs: "oauth-token", cache_expiration=timedelta(minutes=55))
async def get_token(config_block: str = "prefect-worker-client-conf",
                    secret_block: str = "prefect-worker-client-secret") -> str:
    config = (await Secret.load(config_block)).get()
    secret = (await Secret.load(secret_block)).get()

    response = httpx.post(
        config["token-url"],
        data={
            "grant_type": "client_credentials",
            "client_id": config["client-id"],
            "client_secret": secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    response.raise_for_status()
    return response.json()["access_token"]
