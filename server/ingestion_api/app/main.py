from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from app.database import Base, SessionLocal, engine
from app.core.crypto import decrypt_value, encrypt_value
from app.models.token_vault import (
    TokenTypeEnum,
    TokenVault,
    ProcessingBatch,
    TokenizedRecord,
    ProcessedResult,
)

app = FastAPI(title="SDP Ingestion API", version="0.1.0")


# ---------- Database setup ----------


@app.on_event("startup")
def on_startup() -> None:
    """
    For dev: auto-create tables if they don't exist.
    In production, we'll replace this with Alembic migrations.
    """
    Base.metadata.create_all(bind=engine)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------- Health ----------


@app.get("/health")
def health_check() -> dict:
    return {"status": "ok", "service": "ingestion_api"}


# ---------- Dev-only Token Vault endpoints ----------



class TokenVaultCreate(BaseModel):
    original_value: str
    token_value: str
    token_type: TokenTypeEnum
    source_table: str
    source_column: str


@app.post("/dev/token-vault", response_model=dict)
def create_token_record(
    payload: TokenVaultCreate, db: Session = Depends(get_db)
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

    return {"token_id": str(record.token_id)}


@app.get("/dev/token-vault/{token_value}", response_model=dict)
def get_original_by_token(token_value: str, db: Session = Depends(get_db)) -> dict:
    record = (
        db.query(TokenVault)
        .filter(TokenVault.token_value == token_value)
        .order_by(TokenVault.created_at.desc())
        .first()
    )

    if not record:
        raise HTTPException(status_code=404, detail="Token not found")

    original_value = decrypt_value(record.original_value_encrypted)

    return {
        "token_value": record.token_value,
        "token_type": record.token_type,
        "original_value": original_value,
        "source_table": record.source_table,
        "source_column": record.source_column,
    }
# ---------- Ingestion API: /api/v1/process ----------


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
) -> ProcessResponse:
    # If no batch_id provided, create one
    batch_id = payload.batch_id or uuid4()

    # Upsert ProcessingBatch
    batch = db.query(ProcessingBatch).filter(ProcessingBatch.batch_id == batch_id).first()
    if batch is None:
        batch = ProcessingBatch(
            batch_id=batch_id,
            client_id=payload.client_id,
            processing_type=payload.processing_type,
            status="RECEIVED",
        )
        db.add(batch)
    else:
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

    return ProcessResponse(
        batch_id=batch_id,
        accepted_records=count,
        status="RECEIVED",
    )

# ---------- Dev-only processing: simulate analytics on tokenized data ----------


class DevProcessResponse(BaseModel):
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

    # Update batch status
    batch.status = "PROCESSED"
    db.commit()

    return DevProcessResponse(
        batch_id=batch_id,
        processed_records=processed_count,
        model_version="demo_v1",
        status="PROCESSED",
    )

# ---------- Results API: fetch processed results by batch_id ----------


class ResultRecord(BaseModel):
    record_key: Optional[str]
    risk_score: str
    model_version: str


class ResultsResponse(BaseModel):
    batch_id: UUID
    status: str
    records: List[ResultRecord]


@app.get("/api/v1/results/{batch_id}", response_model=ResultsResponse)
def get_results(
    batch_id: UUID,
    db: Session = Depends(get_db),
) -> ResultsResponse:
    batch = (
        db.query(ProcessingBatch)
        .filter(ProcessingBatch.batch_id == batch_id)
        .first()
    )
    if batch is None:
        raise HTTPException(status_code=404, detail="Batch not found")

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

    return ResultsResponse(
        batch_id=batch_id,
        status=batch.status,
        records=out_records,
    )
