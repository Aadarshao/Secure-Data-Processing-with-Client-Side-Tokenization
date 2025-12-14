import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query
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

# -------------------------
# Logging (structured)
# -------------------------

logger = logging.getLogger("ingestion_api")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | ingestion_api | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# -------------------------
# FastAPI app
# -------------------------

app = FastAPI(title="SDP Ingestion API", version="0.2.0")

# -------------------------
# Database setup
# -------------------------

@app.on_event("startup")
def on_startup() -> None:
    """
    For dev: auto-create tables if they don't exist.
    In production, replace with Alembic migrations.
    """
    Base.metadata.create_all(bind=engine)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -------------------------
# Rate limit stub (DEV ONLY)
# -------------------------

RATE_LIMIT_PER_MINUTE = int(os.getenv("SDP_RATE_LIMIT_PER_MINUTE", "60"))
_rate_limit_cache: Dict[str, List[float]] = {}


def check_rate_limit(client_id: str) -> None:
    """
    Simple in-memory rate limit (DEV stub).
    Not suitable for multi-process or production.
    """
    now = time.time()
    window_seconds = 60.0

    timestamps = _rate_limit_cache.get(client_id, [])
    timestamps = [t for t in timestamps if (now - t) < window_seconds]

    if len(timestamps) >= RATE_LIMIT_PER_MINUTE:
        raise HTTPException(status_code=429, detail="Rate limit exceeded for client_id")

    timestamps.append(now)
    _rate_limit_cache[client_id] = timestamps

# -------------------------
# Auth / Client Context (Phase 5.1)
# -------------------------

def _load_api_key_map() -> Dict[str, str]:
    """
    Optional multi-tenant mapping: client_id -> api_key

    Set via env:
      SDP_API_KEYS_JSON='{"bank_demo":"dev-secret-api-key","bank_abc":"another-key"}'

    If this is set, the API key determines the client_id.
    """
    raw = os.getenv("SDP_API_KEYS_JSON")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            return {}
        out: Dict[str, str] = {}
        for k, v in parsed.items():
            out[str(k)] = str(v)
        return out
    except Exception:
        return {}


def _resolve_client_for_key(api_key: str) -> Tuple[Optional[str], str]:
    """
    Returns (authorized_client_id, mode)

    mode:
      - "mapped"    -> SDP_API_KEYS_JSON mapping is used; api_key maps to exactly one client_id
      - "single"    -> SDP_API_KEY used; api_key is valid but not tied to a specific client_id
      - "invalid"   -> not valid
    """
    # 1) Mapped multi-tenant mode (recommended)
    key_map = _load_api_key_map()
    if key_map:
        for client_id, key in key_map.items():
            if api_key == key:
                return client_id, "mapped"
        return None, "invalid"

    # 2) Single shared key mode (dev/simple)
    single_key = os.getenv("SDP_API_KEY")
    if single_key and api_key == single_key:
        return None, "single"

    return None, "invalid"


class ClientContext(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    api_key: str
    client_id: Optional[str]  # if mapped mode, will be the resolved client
    mode: str                 # "mapped" | "single"


def require_client_context(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> ClientContext:
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header")

    client_id, mode = _resolve_client_for_key(x_api_key)
    if mode == "invalid":
        raise HTTPException(status_code=401, detail="Invalid API key")

    return ClientContext(api_key=x_api_key, client_id=client_id, mode=mode)


def _enforce_client_match(
    *,
    ctx: ClientContext,
    requested_client_id: Optional[str],
    batch_client_id: Optional[str],
    action: str,
) -> str:
    """
    Decide the effective client_id and enforce tenant isolation.

    Rules:
      - If mapped mode: ctx.client_id is the ONLY allowed client.
      - If single mode: any client_id is allowed, but we still enforce that
        the requested client matches the batch's stored client for reads/process actions.
      - If requested_client_id is missing, fallback to batch_client_id when available.
    """
    effective_client_id = requested_client_id or batch_client_id

    if not effective_client_id:
        raise HTTPException(
            status_code=400,
            detail="client_id is required (provide in body, query, or X-Client-Id header)",
        )

    # If API key is mapped to a specific client_id, enforce it strictly.
    if ctx.mode == "mapped":
        if ctx.client_id != effective_client_id:
            logger.warning(
                f"auth_denied action={action} reason=api_key_client_mismatch "
                f"authorized_client={ctx.client_id} requested_client={effective_client_id}"
            )
            raise HTTPException(status_code=401, detail="API key not authorized for this client")

    # Tenant isolation against stored batch.client_id (for actions on an existing batch)
    if batch_client_id and effective_client_id != batch_client_id:
        logger.warning(
            f"tenant_denied action={action} reason=cross_tenant_access "
            f"requested_client={effective_client_id} batch_client={batch_client_id}"
        )
        raise HTTPException(status_code=403, detail="Forbidden: cross-tenant access")

    return effective_client_id

# -------------------------
# Health
# -------------------------

@app.get("/health")
def health_check() -> dict:
    return {"status": "ok", "service": "ingestion_api"}

# -------------------------
# Dev-only Token Vault endpoints
# -------------------------

class TokenVaultCreate(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    original_value: str
    token_value: str
    token_type: TokenTypeEnum
    source_table: str
    source_column: str


@app.post("/dev/token-vault", response_model=dict)
def create_token_record(
    payload: TokenVaultCreate,
    db: Session = Depends(get_db),
    ctx: ClientContext = Depends(require_client_context),
) -> dict:
    # Dev endpoint: still requires API key
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

    logger.info(
        "token_vault_created "
        f"client_mode={ctx.mode} token_id={record.token_id} token_type={payload.token_type}"
    )
    return {"token_id": str(record.token_id)}


@app.get("/dev/token-vault/{token_value}", response_model=dict)
def get_original_by_token(
    token_value: str,
    db: Session = Depends(get_db),
    ctx: ClientContext = Depends(require_client_context),
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

    logger.info(
        "token_vault_lookup "
        f"client_mode={ctx.mode} token_value={token_value}"
    )

    return {
        "token_value": record.token_value,
        "token_type": record.token_type,
        "original_value": original_value,
        "source_table": record.source_table,
        "source_column": record.source_column,
    }

# -------------------------
# Ingestion API: /api/v1/process
# -------------------------

class ProcessRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    client_id: str
    processing_type: str
    batch_id: Optional[UUID] = None
    records: List[Dict[str, Any]]


class ProcessResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    batch_id: UUID
    accepted_records: int
    status: str = "ACCEPTED"


@app.post("/api/v1/process", response_model=ProcessResponse)
def process_batch(
    payload: ProcessRequest,
    db: Session = Depends(get_db),
    ctx: ClientContext = Depends(require_client_context),
) -> ProcessResponse:
    effective_client_id = _enforce_client_match(
        ctx=ctx,
        requested_client_id=payload.client_id,
        batch_client_id=None,
        action="process_batch",
    )

    # Rate limit (DEV)
    check_rate_limit(effective_client_id)

    batch_id = payload.batch_id or uuid4()

    # Upsert ProcessingBatch
    batch = db.query(ProcessingBatch).filter(ProcessingBatch.batch_id == batch_id).first()
    if batch is None:
        batch = ProcessingBatch(
            batch_id=batch_id,
            client_id=effective_client_id,
            processing_type=payload.processing_type,
            status="RECEIVED",
        )
        db.add(batch)
    else:
        # If reusing a batch_id, enforce tenant isolation
        effective_client_id = _enforce_client_match(
            ctx=ctx,
            requested_client_id=effective_client_id,
            batch_client_id=batch.client_id,
            action="process_batch_reuse",
        )
        batch.status = "RECEIVED"
        batch.processing_type = payload.processing_type

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

    logger.info(
        "batch_received "
        f"client_id={effective_client_id} batch_id={batch_id} "
        f"processing_type={payload.processing_type} records={count}"
    )

    return ProcessResponse(
        batch_id=batch_id,
        accepted_records=count,
        status="RECEIVED",
    )

# -------------------------
# Dev-only processing (OPTION A: idempotent)
# -------------------------

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
    ctx: ClientContext = Depends(require_client_context),
    client_id: Optional[str] = Query(default=None),
    x_client_id: Optional[str] = Header(default=None, alias="X-Client-Id"),
) -> DevProcessResponse:
    batch = db.query(ProcessingBatch).filter(ProcessingBatch.batch_id == batch_id).first()
    if batch is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    requested_client_id = client_id or x_client_id
    effective_client_id = _enforce_client_match(
        ctx=ctx,
        requested_client_id=requested_client_id,
        batch_client_id=batch.client_id,
        action="dev_process_batch",
    )

    # Rate limit (DEV)
    check_rate_limit(effective_client_id)

    # ✅ OPTION A FIX: make processing idempotent by deleting old results first
    db.query(ProcessedResult).filter(ProcessedResult.batch_id == batch_id).delete(
        synchronize_session=False
    )
    db.commit()

    records = (
        db.query(TokenizedRecord)
        .filter(TokenizedRecord.batch_id == batch_id)
        .all()
    )

    processed_count = 0
    for rec in records:
        payload = rec.payload or {}
        email_token = str(payload.get("email", ""))

        # Dummy scoring function: longer token => higher score
        risk_score_value = str(min(len(email_token), 99))

        record_key = str(payload.get("customer_id")) if "customer_id" in payload else None

        db.add(
            ProcessedResult(
                batch_id=batch_id,
                record_key=record_key,
                risk_score=risk_score_value,
                model_version="demo_v1",
            )
        )
        processed_count += 1

    batch.status = "PROCESSED"
    db.commit()

    logger.info(
        "batch_processed "
        f"client_id={effective_client_id} batch_id={batch_id} "
        f"processed_records={processed_count} model_version=demo_v1"
    )

    return DevProcessResponse(
        batch_id=batch_id,
        processed_records=processed_count,
        model_version="demo_v1",
        status="PROCESSED",
    )

# -------------------------
# Results API: fetch processed results by batch_id
# -------------------------

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
    db: Session = Depends(get_db),
    ctx: ClientContext = Depends(require_client_context),
    client_id: Optional[str] = Query(default=None),
    x_client_id: Optional[str] = Header(default=None, alias="X-Client-Id"),
) -> ResultsResponse:
    batch = db.query(ProcessingBatch).filter(ProcessingBatch.batch_id == batch_id).first()
    if batch is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    requested_client_id = client_id or x_client_id
    effective_client_id = _enforce_client_match(
        ctx=ctx,
        requested_client_id=requested_client_id,
        batch_client_id=batch.client_id,
        action="get_results",
    )

    # Rate limit (DEV)
    check_rate_limit(effective_client_id)

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

    logger.info(
        "results_fetched "
        f"client_id={effective_client_id} batch_id={batch_id} records={len(out_records)}"
    )

    return ResultsResponse(
        batch_id=batch_id,
        status=batch.status,
        records=out_records,
    )
