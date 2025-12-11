███████╗██████╗ ██████╗ 
██╔════╝██╔══██╗██╔══██╗
█████╗  ██████╔╝██║  ██║
██╔══╝  ██╔══██╗██║  ██║
███████╗██║  ██║██████╔╝
╚══════╝╚═╝  ╚═╝╚═════╝  
**SECURE DATA PIPELINE (SDP)**

---


# Secure Data Pipeline (SDP)

## 🎯 Overview

The Secure Data Pipeline (SDP) provides a full workflow for processing sensitive customer data without ever exposing Personally Identifiable Information (PII) to your server.

Data is tokenized locally, uploaded securely, processed server-side, and finally reintegrated locally with the original raw data.

### Key Features

* PII never leaves the client machine
* Encrypted token vault stored locally
* API-authenticated ingestion
* Reversible and non-reversible tokens supported
* Dockerized server + PostgreSQL backend

---

## 🛠️ End-to-End Workflow

1.  End-to-End Tokenization  
2.  Secure Upload  
3.  Server Ingestion  
4.  Analytics  
5.  Local Reintegration  

### System Architecture

```mermaid
flowchart TD
    A[Raw CSV with PII] --> B[Local Tokenization Engine]
    B --> C[Tokenized CSV (Non-PII)]
    C --> D[Secure Upload Client (HTTPS + API Key)]
    D --> E[Ingestion API (FastAPI)]
    E --> F[Postgres Token Vault + Batches]
    E --> G[Dev Processing Engine (Dummy ML Model)]
    G --> H[Results API]
    H --> I[Local Reintegration - Join scores with raw CSV]
```

---

## 1. Client Components

**Location:** `client/sdp_client/`

### 1.1 Tokenization

#### A. Tokenize via Command Line

```bash
python -m sdp_client.cli tokenize-csv \
  --input data/customers_raw.csv \
  --output data/customers_tokenized.csv \
  --source-table customers \
  --column email
```

#### B. Tokenize via YAML Config

```bash
python -m sdp_client.cli tokenize-config \
  --config configs/customers_tokenization.yml \
  --input data/customers_raw.csv \
  --output data/customers_tokenized_from_cfg.csv
```

**Functionality:**

* Generates a tokenized non-PII CSV  
* Stores encrypted originals in a local SQLite vault: `token_vault.db`  
* Uses **AES-GCM** with your crypto key  

```bash
# Example for Windows (PowerShell)
$env:SDP_CRYPTO_KEY = "Miql-SH11OTpm4rFOh5QF7iNG2fPolSwwvvb1YceREw="
```

### 1.2 Upload Batch to Server

Set API key:

```bash
$env:SDP_API_KEY = "dev-secret-api-key"
```

Upload:

```bash
python -m sdp_client.cli upload-batch \
  --file data/customers_tokenized_from_cfg.csv \
  --client-id bank_demo \
  --processing-type risk_scoring \
  --server-url http://localhost:8081
```

Example server response:

```json
{
  "batch_id": "a241d5d5-9746-4f28-bbaa-f465a4f9e5f8",
  "accepted_records": 3,
  "status": "RECEIVED"
}
```

### 1.3 Dev Processing (Simulated ML Scoring)

```powershell
$batchId = "YOUR-BATCH-ID"

Invoke-WebRequest `
  -Uri "http://localhost:8081/dev/process-batch/$batchId" `
  -Method POST |
  Select-Object -ExpandProperty Content
```

Output:

```json
{
  "batch_id": "...",
  "processed_records": 3,
  "model_version": "demo_v1",
  "status": "PROCESSED"
}
```

### 1.4 Retrieve & Integrate Results

```bash
python -m sdp_client.cli integrate-results \
  --batch-id <batch-id> \
  --raw-input data/customers_raw.csv \
  --output data/customers_with_scores.csv \
  --key-column customer_id \
  --server-url http://localhost:8081
```

---

## 2. Server Components

**Location:** `server/ingestion_api/`

### 2.1 Technologies

| Technology | Role |
|-----------|------|
| FastAPI | REST API |
| PostgreSQL | Token vault & metadata |
| SQLAlchemy | ORM |
| AES-GCM | Encryption |
| API Key Auth | Security |
| Docker | Deployment |

### 2.2 Endpoints

| Endpoint | Method | Description |
|---------|--------|-------------|
| `/api/v1/process` | POST | Upload batch |
| `/dev/process-batch/{batch_id}` | POST | Dev scoring |
| `/api/v1/results/{batch_id}` | GET | Retrieve results |
| `/health` | GET | Health check |

---

## 3. Docker Infrastructure

```bash
docker compose up -d
```

Health check:

```bash
curl http://localhost:8081/health
```

---

## 4. Environment Variables

| Variable | Scope | Description |
|----------|--------|-------------|
| `SDP_CRYPTO_KEY` | client/server | AES-GCM |
| `SDP_API_KEY` | client/server | API Key |
| `DB_DSN` | server | PostgreSQL DSN |
| `SDP_ENV` | server | Environment (dev) |

---

## 5. Database Schema

* `token_vault`  
* `processing_batch`  
* `tokenized_record`  
* `processed_result`  

---

## 6. Full Pipeline Example

```bash
python -m sdp_client.cli tokenize-config ...
python -m sdp_client.cli upload-batch ...
Invoke-WebRequest -Uri http://localhost:8081/dev/process-batch/<batchId> -Method POST
python -m sdp_client.cli integrate-results ...
```

---

## 7. Current Status

* Deterministic tokenization  
* AES-GCM vault  
* TLS upload client  
* Ingestion API  
* Dummy scoring engine  
* Full working pipeline  

---

## 8. Future Roadmap

* Production ingestion workers  
* mTLS  
* Advanced ML scoring  
* Real-time ingestion  
* Admin dashboard  
* Key rotation tools  

