# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Infrastructure

Start the full stack (Postgres + Prefect server + worker):
```bash
docker compose up -d
```

Prefect UI is available at `http://localhost:4200`.

Stop everything:
```bash
docker compose down
```

## Running flows locally

```bash
pip install -r requirements.txt
python flows/hello_flow.py
```

Run a flow via Prefect CLI (requires server running):
```bash
prefect run flow --name hello-pipeline
```

## Deploying

Deployments are defined in `prefect.yaml` and pulled from GitHub at run time (clone + pip install). Deploy to the server:
```bash
prefect deploy --all
```

The `local-pool` work pool (process type) must exist on the server; the worker container creates it automatically via `docker compose`.

## Architecture

```
flows/          # Prefect flows (entrypoints for deployments)
common/         # Shared utilities imported by flows
prefect.yaml    # Deployment definitions
docker-compose.yml  # Postgres + Prefect server + process worker
```

**Prefect Variables** are the mechanism for config. Flows load them at runtime:
- `Variable` — JSON string with structured config (e.g. `prefect-worker-conf`, `github-token`)

Variables must be created in the Prefect UI or via CLI before a flow that references them can run.

**`flows/materialize_searches.py`** — calls an internal backend API using a Keycloak token; config (including client secret) is loaded from the `prefect-worker-conf` variable.

**`flows/hello_flow.py`** — reference pipeline with retry-enabled tasks; deployed as `hello-prod` on an hourly cron schedule.
