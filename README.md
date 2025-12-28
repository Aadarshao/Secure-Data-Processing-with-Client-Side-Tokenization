# Secure Data Pipeline (SDP)

## 1. Executive Summary

### 1.1 Product Overview
Secure Data Processor (SDP) is a secure data processing workflow that enables analytics on sensitive customer datasets while keeping Personally Identifiable Information (PII) under client control.

**PII is tokenized locally (client-side)**, only **tokenized data** is uploaded to the server for processing, and results are fetched back and **reintegrated locally** with the original raw dataset.

---

### 1.2 Problem Statement
Organizations face increasing regulatory pressure (GDPR, CCPA, HIPAA) that restricts movement of PII across boundaries. Many existing approaches either:

1. Require centralizing raw PII in a server environment  
2. Use insecure data transfer patterns  
3. Do not support round-trip workflows (process results and re-join with local PII)

SDP demonstrates a practical and compliant solution:

- **PII never leaves the client machine**
- Servers process only **tokenized representations**
- Results are merged **locally** with the original dataset

---

## 2. Key Features

- ✅ Client-side tokenization (PII stays local)
- ✅ Encrypted local token vault (SQLite + AES-GCM)
- ✅ API key authenticated ingestion (`X-API-Key`)
- ✅ Supported token types:
  - **HASH** (deterministic; referential integrity)
  - **MASKED** (non-reversible, shape-preserving)
  - **RANDOM** (non-deterministic; anonymization)
  - **FPE** (stub / future work)
- ✅ TLS support (Caddy reverse proxy + local CA bundle)
- ✅ Dockerized server with PostgreSQL persistence
- ✅ End-to-end pipeline: upload → process → fetch → integrate locally

> **Note:** Kubernetes is intentionally not used. Docker Compose is the intended deployment method.

---

## 3. System Architecture

### 3.1 Data Flow (Outbound + Inbound)

#### Flow 1: Outbound Processing
1. Extract: Client loads raw CSV containing PII  
2. Tokenize: PII fields are converted to tokens locally  
3. Package: Tokenized CSV is generated  
4. Transfer: Tokenized CSV is uploaded over TLS  
5. Process: Server runs analytics on tokenized data  
6. Store: Results stored server-side by `batch_id` and `client_id`

#### Flow 2: Inbound Results
1. Receive: Client fetches results using `batch_id`  
2. Integrate: Results joined with raw CSV locally  
3. Output: Final merged dataset written to disk  

---

### 3.2 Architecture Diagram

```mermaid
flowchart TD
    A["Raw CSV (PII)"]
    B["Local Tokenization (Client)"]
    C["Tokenized CSV (No PII)"]
    D["TLS Upload + API Key"]
    E["Ingestion API (FastAPI)"]
    F["PostgreSQL (Tokenized Data + Batches)"]
    G["Processing Engine (Dev Processor / Spark)"]
    H["Results API"]
    I["Client Fetch + Local Integration"]
    J["Merged Output CSV (PII + Scores)"]

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    E --> G
    G --> H
    H --> I
    I --> J
 ```   
## 4. Repository Structure
text
```
client/                 # Client-side tokenization + CLI
server/ingestion_api/   # FastAPI ingestion + results APIs
infra/                  # Docker Compose stack + Caddy TLS proxy
```
## 5. Quickstart (End-to-End Demo)
### 5.1 Start Infrastructure (Docker)
From the repository root:

bash
```
docker compose --env-file .env -f infra/docker-compose.yml up -d --build --force-recreate
```
Expected containers:

sdp_postgres – PostgreSQL

sdp_ingestion_api – FastAPI service

sdp_caddy – TLS reverse proxy

### 5.2 Generate Local TLS Root CA Bundle (Windows PowerShell)
This writes the Caddy root certificate to:

bash
```
infra/caddy/pki/caddy-root.crt
```
powershell
```
powershell -ExecutionPolicy Bypass -File infra/scripts/gen-local-tls.ps1
```
### 5.3 Client Environment Configuration
Create client/.env:

env
```
SDP_SERVER_URL=https://localhost:8443
SDP_CA_BUNDLE=D:\Projects\sdp\infra\caddy\pki\caddy-root.crt
SDP_API_KEY=dev-secret-api-key
SDP_CRYPTO_KEY=Miql-SH11OTpm4rFOh5QF7iNG2fPolSwwvvb1YceREw=
```
The CLI automatically loads environment variables from both the repo root .env and client/.env.

### 5.4 Upload Tokenized CSV
bash
```
python -m sdp_client.cli upload-batch `
  --file "D:\Projects\sdp\data\masked.csv" `
  --client-id bank_demo `
  --processing-type risk_scoring
```
Example response:

json
```
{
  "batch_id": "a1180fb1-caea-4893-9219-f429141d0929",
  "accepted_records": 3,
  "status": "RECEIVED"
}
```
### 5.5 Process Batch (Dev Processor)
bash
```
python -m sdp_client.cli process-batch `
  --raw-input "D:\Projects\sdp\data\raw.csv" `
  --output "D:\Projects\sdp\data\customers_with_scores.csv" `
  --key-column customer_id `
  --tokenized-file "D:\Projects\sdp\data\masked.csv" `
  --client-id bank_demo `
  --processing-type risk_scoring `
  --use-dev-processor `
  --jobs-mount "D:\Projects\sdp\jobs" `
  --db-url "postgresql://sdp:sdp_password@localhost:5432/sdp" `
  --db-user sdp `
  --db-password sdp_password
```
This performs:

Upload

Dev-only processing

Polling until PROCESSED

Local integration into output CSV

## 6. Client Components
Location: client/src/sdp_client/

### 6.1 Tokenization
Tokenization occurs entirely on the client and produces:

A tokenized CSV

An encrypted local token vault

A. Tokenize a Single Column
bash
```
python -m sdp_client.cli tokenize-csv \
  --input data/raw.csv \
  --output data/tokenized.csv \
  --source-table customers \
  --column email \
  --token-type HASH
```
B. Tokenize via YAML Configuration
bash
```
python -m sdp_client.cli tokenize-config \
  --config configs/customers_tokenization.yml \
  --input data/raw.csv \
  --output data/tokenized_from_cfg.csv
```
### 6.2 Supported Token Types
Token Type	Description
HASH	Deterministic SHA-256 token; preserves referential integrity
MASKED	Non-reversible masking; preserves format (email domain, last-4 digits)
RANDOM	Non-deterministic anonymized tokens
FPE	Stub / future implementation

## 7. Server Components
Location: server/ingestion_api/

### 7.1 Technology Stack
Technology	Role
FastAPI	REST API
PostgreSQL	Persistence for batches and results
SQLAlchemy	ORM
AES-GCM	Encryption for token vault
Docker Compose	Deployment

### 7.2 Key API Endpoints
Endpoint	Method	Description
/api/v1/process	POST	Upload JSON batch
/api/v1/process-file	POST	Upload tokenized CSV
/dev/process-batch/{batch_id}	POST	Dev-only simulated processing
/api/v1/results/{batch_id}	GET	Fetch results
/health	GET	Liveness probe
/readyz	GET	Database readiness

### 7.3 Dev Endpoint Guardrail
All /dev/* endpoints:

Are enabled only when SDP_ENV=dev

Return 404 Not Found in non-dev environments

This prevents accidental exposure of development processing logic in production.

## 8. Integration Rules (Result Merge)
When integrating results into the raw CSV:

Join key: --key-column (default: customer_id)

All merging occurs locally

Missing keys in results → raw rows remain unchanged

Duplicate keys in raw CSV → all matching rows updated

This behavior is intentional and documented.

## 9. Environment Variables
Variable	Scope	Description
SDP_CRYPTO_KEY	Client	AES-GCM key for local vault
SDP_API_KEY	Client + Server	API key sent via X-API-Key
SDP_SERVER_URL	Client	Base ingestion API URL
SDP_CA_BUNDLE	Client	TLS CA bundle path
DB_DSN	Server	PostgreSQL DSN
SDP_ENV	Server	Environment mode (dev recommended)

## 10. Requirements Compliance Matrix
Requirement	Priority	Status
Client-side tokenization	P0	✅
Secure bi-directional transfer	P0	✅
Server-side tokenized processing	P0	✅
Client-side result integration	P0	✅

## 11. Future Roadmap
Proper FPE (FF1 / FF3) backed by KMS or HSM

Mutual TLS (client certificates)

Chunked / resumable uploads for large datasets

Distributed rate limiting (Redis)

Append-only audit stream (Kafka)

Observability: metrics, logging, tracing

Real-time ingestion pipeline