# Secure Data Processor (SDP) – Local Dev Skeleton

## Prerequisites

- Python 3.11
- Docker Desktop (with docker compose)
- Git

## Structure

- `client/` – client-side tools (tokenization, transfer) – TBD in later phases
- `server/ingestion_api/` – FastAPI ingestion API
- `infra/` – docker-compose for Postgres + services

## How to run the server stack

From the `infra/` directory:

```bash
cd infra
docker compose up --build
