import logging
import os
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from app.database import Base, SessionLocal, engine
from app.core.crypto import decrypt_value, encrypt_value
from app.models.token_vault import (
    TokenTypeEnum,
    TokenVault,
    ProcessingBatch,
    TokenizedRecord,
    ProcessedResult,
)

# -----------------------------------------------------------------------------
# App + Logging setup
# -----------------------------------------------------------------------------

app = FastAPI(title="SDP Ingestion API", version="0.1.0")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("ingestion_api")


def log_event(event: str, **fields: Any) -> None:
    """
    Small helper for structured log-style messages.
    Example:
      log_event("batch_received", client_id="bank_demo", batch_id="...", records=3)
    """
    extras = " ".join(f"{k}={v}" for k, v in fields.items())
    logger.info("%s %s", event, extras)


# -----------------------------------------------------------------------------
# API key auth
# -----------------------------------------------------------------------------

API_KEY = os.getenv("SDP_API_KEY")


def verify_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> str:
    """
    Simple header-based API key check.
    - Expects header: X-API-Key: <API_KEY>
    """
    if not API_KEY:
        # If no API key configured, we accept whatever is given (dev convenience)
        return x_api_key

    if x_api_key != API_KEY:
        log_event("invalid_api_key", provided_key=x_api_key)
        raise HTTPException(status_code=401, detail="Invalid API key")

    return x_api_key


# -----------------------------------------------------------------------------
# Simple in-memory per-client rate limiter (stub)
# -----------------------------------------------------------------------------

RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("SDP_RATE_LIMIT_WINDOW_SECONDS", "60"))
RATE_LIMIT_MAX_REQUESTS = int(os.getenv("SDP_RATE_LIMIT_MAX_REQUESTS", "60"))

# Keyed by (client_id, scope) -> deque[timestamps]
RateLimitKey = Tuple[str, str]
_rate_limit_buckets: Dict[RateLimitKey, Deque[float]] = defaultdict(deque)


def enforce_rate_limit(client_id: str, scope: str) -> None:
    """
    Very simple in-memory rate limit stub, per client_id + scope.
    Not distributed and not persistent – good enough for this demo.
    """
    if not client_id:
        # If somehow empty, don't rate-limit (but log)
        log_event("rate_limit_missing_client_id", scope=scope)
        return

    now = time.time()
    key: RateLimitKey = (client_id, scope)
    bucket = _rate_limit_buckets[key]

    # Drop timestamps outside the window
    while bucket and (now - bucket[0] > RATE_LIMIT_WINDOW_SECONDS):
        bucket.popleft()

    if len(bucket) >= RATE_LIMIT_MAX_REQUESTS:
        log_event(
            "rate_limit_exceeded",
            client_id=client_id,
            scope=scope,
            window_seconds=RATE_LIMIT_WINDOW_SECONDS,
            max_requests=RATE_LIMIT_MAX_REQUESTS,
        )
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded for this client. Please slow down.",
        )

    bucket.append(now)


# -----------------------------------------------------------------------------
# Database setup
# -----------------------------------------------------------------------------

@app.on_event("startup")
def on_startup() -> None:
    """
    For dev: auto-create tables if they don't exist.
    In production, we'll replace this with Alembic migrations.
    """
    Base.metadata.create_all(bind=engine)
    log_event(
        "startup",
        rate_limit_window_seconds=RATE_LIMIT_WINDOW_SECONDS,
        rate_limit_max_requests=RATE_LIMIT_MAX_REQUESTS,
    )


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------

@app.get("/health")
def health_check() -> dict:
    return {"status": "ok", "service": "ingestion_api"}


# -----------------------------------------------------------------------------
# Dev-only Token Vault endpoints
# -----------------------------------------------------------------------------

class TokenVaultCreate(BaseModel):
    original_value: str
    token_value: str
    token_type: TokenTypeEnum
    source_table: str
    source_column: str


@app.post("/dev/token-vault", response_model=dict)
def create_token_record(
    payload: TokenVaultCreate,
    db: Session = Depends(get_db),
) -> dict:
    encrypted = encrypt_value(payload.original_value)

    record = TokenVault(
        original_value_encrypted=encrypted,
        token_value=payload.token_value,
        token_type=payload.token_type,
        source_table=payload.source_table,
        source_column=payload.source_column,
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    log_event(
        "dev_token_created",
        token_id=str(record.token_id),
        source_table=record.source_table,
        source_column=record.source_column,
    )

    return {"token_id": str(record.token_id)}


@app.get("/dev/token-vault/{token_value}", response_model=dict)
def get_original_by_token(
    token_value: str,
    db: Session = Depends(get_db),
) -> dict:
    record = (
        db.query(TokenVault)
        .filter(TokenVault.token_value == token_value)
        .order_by(TokenVault.created_at.desc())
        .first()
    )

    if not record:
        raise HTTPException(status_code=404, detail="Token not found")

    original_value = decrypt_value(record.original_value_encrypted)

    log_event(
        "dev_token_lookup",
        token_value=token_value,
        source_table=record.source_table,
        source_column=record.source_column,
    )

    return {
        "token_value": record.token_value,
        "token_type": record.token_type,
        "original_value": original_value,
        "source_table": record.source_table,
        "source_column": record.source_column,
    }


# -----------------------------------------------------------------------------
# Ingestion API: /api/v1/process
# -----------------------------------------------------------------------------

class TokenizedRecordIn(BaseModel):
    data: Dict[str, Any]


class ProcessRequest(BaseModel):
    client_id: str
    processing_type: str
    batch_id: Optional[UUID] = None
    records: List[Dict[str, Any]]


class ProcessResponse(BaseModel):
    batch_id: UUID
    accepted_records: int
    status: str = "ACCEPTED"


@app.post("/api/v1/process", response_model=ProcessResponse)
def process_batch(
    payload: ProcessRequest,
    db: Session = Depends(get_db),
    _: str = Depends(verify_api_key),
) -> ProcessResponse:
    # Rate limit per client for ingestion
    enforce_rate_limit(payload.client_id, scope="process")

    # If no batch_id provided, create one
    batch_id = payload.batch_id or uuid4()

    # Upsert ProcessingBatch
    batch = (
        db.query(ProcessingBatch)
        .filter(ProcessingBatch.batch_id == batch_id)
        .first()
    )
    if batch is None:
        batch = ProcessingBatch(
            batch_id=batch_id,
            client_id=payload.client_id,
            processing_type=payload.processing_type,
            status="RECEIVED",
        )
        db.add(batch)
    else:
        # Guardrail: ensure client_id isn't silently changed across reuses
        if batch.client_id != payload.client_id:
            log_event(
                "batch_client_mismatch",
                existing_client_id=batch.client_id,
                payload_client_id=payload.client_id,
                batch_id=batch_id,
            )
            raise HTTPException(
                status_code=400,
                detail="client_id does not match existing batch owner",
            )
        batch.status = "RECEIVED"

    # Store each tokenized record as JSONB
    count = 0
    for record in payload.records:
        db.add(
            TokenizedRecord(
                batch_id=batch_id,
                payload=record,
            )
        )
        count += 1

    db.commit()

    log_event(
        "batch_received",
        client_id=payload.client_id,
        batch_id=str(batch_id),
        processing_type=payload.processing_type,
        records=count,
    )

    return ProcessResponse(
        batch_id=batch_id,
        accepted_records=count,
        status="RECEIVED",
    )


# -----------------------------------------------------------------------------
# Dev-only processing: simulate analytics on tokenized data
# -----------------------------------------------------------------------------

class DevProcessResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    batch_id: UUID
    processed_records: int
    model_version: str = "demo_v1"
    status: str = "PROCESSED"


@app.post("/dev/process-batch/{batch_id}", response_model=DevProcessResponse)
def dev_process_batch(
    batch_id: UUID,
    db: Session = Depends(get_db),
) -> DevProcessResponse:
    # Load batch
    batch = (
        db.query(ProcessingBatch)
        .filter(ProcessingBatch.batch_id == batch_id)
        .first()
    )
    if batch is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    # Fetch tokenized records for this batch
    records = (
        db.query(TokenizedRecord)
        .filter(TokenizedRecord.batch_id == batch_id)
        .all()
    )

    processed_count = 0

    for rec in records:
        # Very simple "model": risk_score based on token length
        # (works entirely on tokenized/non-PII data)
        payload = rec.payload or {}
        email_token = str(payload.get("email", ""))

        # Dummy scoring function: longer token => higher score
        risk_score_value = str(min(len(email_token), 99))

        # Optional record_key: use customer_id if present
        record_key = (
            str(payload.get("customer_id"))
            if "customer_id" in payload
            else None
        )

        db.add(
            ProcessedResult(
                batch_id=batch_id,
                record_key=record_key,
                risk_score=risk_score_value,
                model_version="demo_v1",
            )
        )
        processed_count += 1

    # Update batch status
    batch.status = "PROCESSED"
    db.commit()

    log_event(
        "batch_processed",
        client_id=batch.client_id,
        batch_id=str(batch_id),
        processed_records=processed_count,
        model_version="demo_v1",
    )

    return DevProcessResponse(
        batch_id=batch_id,
        processed_records=processed_count,
        model_version="demo_v1",
        status="PROCESSED",
    )


# -----------------------------------------------------------------------------
# Results API: fetch processed results by batch_id
# -----------------------------------------------------------------------------

class ResultRecord(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    record_key: Optional[str]
    risk_score: str
    model_version: str


class ResultsResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    batch_id: UUID
    status: str
    records: List[ResultRecord]


@app.get("/api/v1/results/{batch_id}", response_model=ResultsResponse)
def get_results(
    batch_id: UUID,
    x_client_id: str = Header(..., alias="X-Client-Id"),
    db: Session = Depends(get_db),
    _: str = Depends(verify_api_key),
) -> ResultsResponse:
    """
    Multi-tenant guardrail:
    - Requires X-Client-Id header.
    - Ensures that the batch belongs to this client_id.
    """
    client_id = x_client_id

    # Rate limit per client for results queries
    enforce_rate_limit(client_id, scope="results")

    batch = (
        db.query(ProcessingBatch)
        .filter(ProcessingBatch.batch_id == batch_id)
        .first()
    )
    if batch is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    # Tenant isolation check
    if batch.client_id != client_id:
        log_event(
            "results_forbidden_wrong_client",
            requested_client_id=client_id,
            batch_client_id=batch.client_id,
            batch_id=str(batch_id),
        )
        raise HTTPException(
            status_code=403,
            detail="Forbidden: batch does not belong to this client_id",
        )

    results = (
        db.query(ProcessedResult)
        .filter(ProcessedResult.batch_id == batch_id)
        .all()
    )

    out_records: List[ResultRecord] = [
        ResultRecord(
            record_key=r.record_key,
            risk_score=r.risk_score,
            model_version=r.model_version,
        )
        for r in results
    ]

    log_event(
        "results_fetched",
        client_id=client_id,
        batch_id=str(batch_id),
        records=len(out_records),
    )

    return ResultsResponse(
        batch_id=batch_id,
        status=batch.status,
        records=out_records,
    )
