import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from app.database import Base


class TokenTypeEnum(str, enum.Enum):
    FPE = "FPE"
    HASH = "HASH"
    MASKED = "MASKED"
    RANDOM = "RANDOM"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ProcessingBatch(Base):
    __tablename__ = "processing_batches"

    batch_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(String(100), nullable=False)
    processing_type = Column(String(100), nullable=False)
    status = Column(String(50), nullable=False, default="PENDING")
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
        onupdate=utcnow,
    )

    # Tokens produced for this batch (not strictly required for demo)
    token_records = relationship("TokenVault", back_populates="batch")

    # Tokenized records stored as JSONB (input to analytics)
    tokenized_records = relationship(
        "TokenizedRecord",
        back_populates="batch",
        cascade="all, delete-orphan",
    )

    # Processed results (output of analytics)
    processed_results = relationship(
        "ProcessedResult",
        back_populates="batch",
        cascade="all, delete-orphan",
    )

    # Audit events for this batch
    audit_events = relationship(
        "BatchAuditEvent",
        back_populates="batch",
        cascade="all, delete-orphan",
    )


class TokenVault(Base):
    __tablename__ = "token_vault"

    token_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Encrypted original PII
    original_value_encrypted = Column(Text, nullable=False)

    # Token representing this PII (deterministic for referential integrity)
    token_value = Column(String(255), nullable=False, index=True)

    token_type = Column(Enum(TokenTypeEnum), nullable=False)

    source_table = Column(String(255), nullable=False)
    source_column = Column(String(255), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    expires_at = Column(DateTime(timezone=True), nullable=True)

    batch_id = Column(
        UUID(as_uuid=True),
        ForeignKey("processing_batches.batch_id"),
        nullable=True,
    )
    batch = relationship("ProcessingBatch", back_populates="token_records")


class TokenizedRecord(Base):
    """
    Stores the tokenized, non-PII payload that is sent to the analytics pipeline.
    """
    __tablename__ = "tokenized_records"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    batch_id = Column(
        UUID(as_uuid=True),
        ForeignKey("processing_batches.batch_id"),
        nullable=False,
        index=True,
    )

    # Arbitrary JSON payload (e.g. tokenized CSV row)
    payload = Column(JSONB, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    batch = relationship("ProcessingBatch", back_populates="tokenized_records")


class ProcessedResult(Base):
    """
    Stores the analytics results (e.g. risk_score) for each record in a batch.
    """
    __tablename__ = "processed_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    batch_id = Column(
        UUID(as_uuid=True),
        ForeignKey("processing_batches.batch_id"),
        nullable=False,
        index=True,
    )

    # Optional logical key (e.g. customer_id)
    record_key = Column(String(255), nullable=True)

    # Demo: store risk_score as a string
    risk_score = Column(String(50), nullable=False)

    model_version = Column(String(50), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    batch = relationship("ProcessingBatch", back_populates="processed_results")


class BatchAuditEvent(Base):
    """
    Simple audit log for each batch event (ingest, process, results fetch, etc.).
    """
    __tablename__ = "batch_audit_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    batch_id = Column(
        UUID(as_uuid=True),
        ForeignKey("processing_batches.batch_id"),
        nullable=False,
        index=True,
    )
    client_id = Column(String(100), nullable=False)

    # e.g. "INGEST_RECEIVED", "BATCH_PROCESSED", "RESULTS_FETCHED"
    event_type = Column(String(100), nullable=False)

    # Arbitrary JSON details (counts, model_version, etc.)
    details = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    batch = relationship("ProcessingBatch", back_populates="audit_events")
