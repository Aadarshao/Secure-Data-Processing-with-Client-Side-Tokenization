Secure Data Pipeline (SDP)
End-to-End Tokenization → Secure Upload → Server Ingestion → Analytics → Local Reintegration
 Overview

The Secure Data Pipeline (SDP) provides a full workflow for processing sensitive customer data without ever exposing PII to your server.
Data is tokenized locally, uploaded securely, processed server-side, and finally reintegrated locally with original raw data.

 PII never leaves the client machine
 Encrypted token vault stored locally
 API-authenticated ingestion
 Reversible and non-reversible tokens supported
 Dockerized server + PostgreSQL backend

 System Architecture
flowchart TD

A[Raw CSV with PII] --> B[Local Tokenization Engine]
B --> C[Tokenized CSV (Non-PII)]
C --> D[Secure Upload Client<br>(HTTPS + API Key)]
D --> E[Ingestion API (FastAPI)]
E --> F[Postgres Token Vault + Batches]
E --> G[Dev Processing Engine<br>(Dummy ML Model)]
G --> H[Results API]
H --> I[Local Reintegration<br>Join scores with raw CSV]

1. Client Components

Located in: client/sdp_client/

 1.1 Tokenization
Tokenize CSV by specifying a single column
python -m sdp_client.cli tokenize-csv \
  --input data/customers_raw.csv \
  --output data/customers_tokenized.csv \
  --source-table customers \
  --column email

Tokenize via YAML config

configs/customers_tokenization.yml defines all tokenization rules.

Run:

python -m sdp_client.cli tokenize-config \
  --config configs/customers_tokenization.yml \
  --input data/customers_raw.csv \
  --output data/customers_tokenized_from_cfg.csv


This:

Generates a tokenized non-PII CSV

Stores encrypted originals in local SQLite vault: token_vault.db

Uses AES-GCM with your crypto key:

$env:SDP_CRYPTO_KEY = "Miql-SH11OTpm4rFOh5QF7iNG2fPolSwwvvb1YceREw="

 1.2 Upload Batch to Server

Set your API key (required):

$env:SDP_API_KEY = "dev-secret-api-key"


Upload a batch:

python -m sdp_client.cli upload-batch \
  --file data/customers_tokenized_from_cfg.csv \
  --client-id bank_demo \
  --processing-type risk_scoring \
  --server-url http://localhost:8081


Example server response:

{
  "batch_id": "a241d5d5-9746-4f28-bbaa-f465a4f9e5f8",
  "accepted_records": 3,
  "status": "RECEIVED"
}

 1.3 Dev Processing (simulated ML scoring)

Trigger server-side processing:

$batchId = "YOUR-BATCH-ID"

Invoke-WebRequest `
  -Uri "http://localhost:8081/dev/process-batch/$batchId" `
  -Method POST |
  Select-Object -ExpandProperty Content


Expected output:

{
  "batch_id": "...",
  "processed_records": 3,
  "model_version": "demo_v1",
  "status": "PROCESSED"
}

📥 1.4 Retrieve & Integrate Results with Raw CSV
python -m sdp_client.cli integrate-results \
  --batch-id <batch-id> \
  --raw-input data/customers_raw.csv \
  --output data/customers_with_scores.csv \
  --key-column customer_id \
  --server-url http://localhost:8081


This produces:

customers_with_scores.csv


containing:

Raw customer data

Model risk scores

Model version metadata

2. Server Components

Located in: server/ingestion_api/

🗄️ 2.1 Technologies

FastAPI (REST API)

PostgreSQL (token vault + batch metadata)

SQLAlchemy ORM

AES-GCM encrypted original values

API key authentication (X-API-Key)

Dockerized environment

📡 2.2 Key Endpoints
POST /api/v1/process

Upload a batch of tokenized records.

POST /dev/process-batch/{batch_id}

Dev-only simulated ML scoring.

GET /api/v1/results/{batch_id}

Return processed scores for a batch.

GET /health

Simple health probe.

3. Docker Infrastructure

Located in: infra/docker-compose.yml

Start full stack:

docker compose up -d


Check service:

curl http://localhost:8081/health


Expected:

{"status":"ok","service":"ingestion_api"}

4. Environment Variables
Variable	Scope	Description
SDP_CRYPTO_KEY	client + server	AES-GCM encryption key
SDP_API_KEY	client + server	API key (header X-API-Key)
DB_DSN	server	PostgreSQL DSN
SDP_ENV	server	Environment mode (dev)
5. Database Schema
Tables

token_vault – encrypted originals + tokens

processing_batch – tracking client uploads

tokenized_record – each uploaded tokenized row

processed_result – scoring results

6. End-to-End Example (Full Pipeline)
# 1) Tokenize
python -m sdp_client.cli tokenize-config --config configs/customers_tokenization.yml ...

# 2) Upload tokenized CSV
python -m sdp_client.cli upload-batch ...

# 3) Process (dev)
Invoke-WebRequest -Uri http://localhost:8081/dev/process-batch/<batchId> -Method POST

# 4) Integrate results with raw PII
python -m sdp_client.cli integrate-results ...


Result:

✔ Raw PII stays local
✔ Tokenized data processed securely
✔ Scores merged locally

7. Current Status

This implementation currently provides:

✔ Local deterministic & config-driven tokenization
✔ AES-GCM encrypted token vault
✔ Upload client with TLS support
✔ Server ingestion API
✔ Dev-only ML simulator
✔ End-to-end tested integration

8. Future Roadmap

Production ingestion workers (Celery / Sidekiq / SQS)

mTLS between client ↔ server

Advanced ML scoring pipeline

Stream/real-time ingestion

Admin dashboard for batches

Secure key rotation tools