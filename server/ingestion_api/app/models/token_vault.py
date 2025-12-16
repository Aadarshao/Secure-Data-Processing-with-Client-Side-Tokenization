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
    UniqueConstraint,
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

    token_records = relationship("TokenVault", back_populates="batch")

    tokenized_records = relationship(
        "TokenizedRecord",
        back_populates="batch",
        cascade="all, delete-orphan",
    )

    processed_results = relationship(
        "ProcessedResult",
        back_populates="batch",
        cascade="all, delete-orphan",
    )

    audit_events = relationship(
        "BatchAuditEvent",
        back_populates="batch",
        cascade="all, delete-orphan",
    )


class TokenVault(Base):
    __tablename__ = "token_vault"

    token_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    original_value_encrypted = Column(Text, nullable=False)
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
    Stores the tokenized payload sent to analytics pipeline.
    Idempotency key: (batch_id, record_key)
    """
    __tablename__ = "tokenized_records"

    __table_args__ = (
        UniqueConstraint("batch_id", "record_key", name="uq_tokenized_batch_recordkey"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    batch_id = Column(
        UUID(as_uuid=True),
        ForeignKey("processing_batches.batch_id"),
        nullable=False,
        index=True,
    )

    record_key = Column(String(255), nullable=False, index=True)

    payload = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    batch = relationship("ProcessingBatch", back_populates="tokenized_records")


class ProcessedResult(Base):
    """
    Stores analytics results per record in a batch.
    Idempotency key: (batch_id, record_key)
    """
    __tablename__ = "processed_results"

    __table_args__ = (
        UniqueConstraint("batch_id", "record_key", name="uq_results_batch_recordkey"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    batch_id = Column(
        UUID(as_uuid=True),
        ForeignKey("processing_batches.batch_id"),
        nullable=False,
        index=True,
    )

    record_key = Column(String(255), nullable=False, index=True)

    risk_score = Column(String(50), nullable=False)
    model_version = Column(String(50), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    batch = relationship("ProcessingBatch", back_populates="processed_results")


class BatchAuditEvent(Base):
    __tablename__ = "batch_audit_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    batch_id = Column(
        UUID(as_uuid=True),
        ForeignKey("processing_batches.batch_id"),
        nullable=False,
        index=True,
    )
    client_id = Column(String(100), nullable=False)
    event_type = Column(String(100), nullable=False)
    details = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    batch = relationship("ProcessingBatch", back_populates="audit_events")
