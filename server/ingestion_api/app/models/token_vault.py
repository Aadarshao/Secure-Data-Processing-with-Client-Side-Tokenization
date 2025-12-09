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
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql import JSONB
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

class TokenizedRecord(Base):
    __tablename__ = "tokenized_records"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    batch_id = Column(
        UUID(as_uuid=True),
        ForeignKey("processing_batches.batch_id"),
        nullable=False,
        index=True,
    )
    payload = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    batch = relationship("ProcessingBatch")


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

class ProcessedResult(Base):
    __tablename__ = "processed_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    batch_id = Column(
        UUID(as_uuid=True),
        ForeignKey("processing_batches.batch_id"),
        nullable=False,
        index=True,
    )

    # e.g. primary key / business key to link back to row
    record_key = Column(String(255), nullable=True)

    # Model output only (no raw PII)
    risk_score = Column(String(50), nullable=False)
    model_version = Column(String(50), nullable=False, default="demo_v1")

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    batch = relationship("ProcessingBatch")
