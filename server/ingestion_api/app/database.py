import os

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Use DB_DSN from environment (set in docker-compose)
DB_DSN = os.getenv("DB_DSN", "postgresql://sdp:sdp_password@postgres:5432/sdp")

engine = create_engine(
    DB_DSN,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
