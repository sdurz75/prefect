# common/keycloak.py
from prefect.blocks.system import Secret, JSON
import httpx

@task(cache_key_fn=lambda *args, **kwargs: "oauth-token", cache_expiration=timedelta(minutes=55))
def get_token(config_block: str = "prefect-worker-client-conf", 
              secret_block: str = "prefect-worker-client-secret") -> str:
    config = JSON.load(config_block).value
    secret = Secret.load(secret_block).get()
    
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

