#!/usr/bin/env python3
"""
SDP Spark Processor (Phase 4) - Updated "works end-to-end" version

Fixes included (based on your logs):
1) UUID mismatch on Postgres insert:
   - Spark DataFrame columns are strings; Postgres table expects UUID.
   - Solution: write into a staging table (all TEXT), then INSERT ... SELECT with ::uuid cast.

2) ClassNotFoundException: org.postgresql.Driver in custom JDBC SQL runner:
   - Avoid Class.forName() completely; use DriverManager directly (driver already on Spark classpath).

3) Idempotency:
   - We keep left_anti join (Spark-side).
   - PLUS we do ON CONFLICT DO NOTHING (DB-side) for true safety if you add unique constraint.

4) Robustness:
   - Uses parameterized JDBC options in one place.
   - Creates staging table if missing.
   - Clears only this batch from staging, then inserts this batch into processed_results.

Requirements:
- Your Spark submit must include Postgres JDBC jar, e.g.
  spark-submit --jars /jobs/jars/postgresql.jar /jobs/spark_processor.py ...

Recommended DB constraint (strongly):
  ALTER TABLE processed_results
  ADD CONSTRAINT processed_results_uniq UNIQUE (batch_id, record_key);
"""

import argparse
import sys
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, length, least, lit, trim


STAGING_TABLE = "processed_results_stage"


def _jdbc_base_options(args) -> Dict[str, str]:
    return {
        "url": args.db_url,
        "user": args.db_user,
        "password": args.db_password,
        "driver": args.db_driver,
        # Optional tuning knobs:
        # "fetchsize": "10000",
    }


def _read_jdbc_query(spark: SparkSession, args, query: str) -> DataFrame:
    opts = _jdbc_base_options(args)
    return (
        spark.read.format("jdbc")
        .option("url", opts["url"])
        .option("query", query)
        .option("user", opts["user"])
        .option("password", opts["password"])
        .option("driver", opts["driver"])
        .load()
    )


def _write_jdbc_table(df: DataFrame, args, table: str, mode: str = "append") -> None:
    opts = _jdbc_base_options(args)
    (
        df.write.format("jdbc")
        .option("url", opts["url"])
        .option("dbtable", table)
        .option("user", opts["user"])
        .option("password", opts["password"])
        .option("driver", opts["driver"])
        .mode(mode)
        .save()
    )


def _run_sql_via_jdbc(spark: SparkSession, args, sql: str) -> None:
    """
    Executes SQL via JVM DriverManager.
    IMPORTANT: Do NOT call Class.forName(); Spark already has the driver on its classpath.
    """
    jvm = spark._sc._gateway.jvm  # type: ignore[attr-defined]
    DriverManager = jvm.java.sql.DriverManager

    conn = DriverManager.getConnection(args.db_url, args.db_user, args.db_password)
    try:
        stmt = conn.createStatement()
        try:
            stmt.execute(sql)
        finally:
            stmt.close()
    finally:
        conn.close()


def _ensure_staging_table(spark: SparkSession, args) -> None:
    """
    Create a staging table with TEXT columns so Spark can always write without type conflicts.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
        batch_id TEXT NOT NULL,
        record_key TEXT NOT NULL,
        risk_score TEXT NOT NULL,
        model_version TEXT NOT NULL
    );
    """
    _run_sql_via_jdbc(spark, args, sql)


def _clear_staging_for_batch(spark: SparkSession, args) -> None:
    sql = f"DELETE FROM {STAGING_TABLE} WHERE batch_id = '{args.batch_id}';"
    _run_sql_via_jdbc(spark, args, sql)


def _insert_into_final_from_staging(spark: SparkSession, args) -> None:
    """
    Insert from staging into final table while casting batch_id to uuid.
    Also trims to be safe.

    If you have a UNIQUE constraint on (batch_id, record_key), this will be fully idempotent.
    """
    sql = f"""
    INSERT INTO processed_results (batch_id, record_key, risk_score, model_version)
    SELECT
        trim(batch_id)::uuid,
        trim(record_key),
        trim(risk_score),
        trim(model_version)
    FROM {STAGING_TABLE}
    WHERE batch_id = '{args.batch_id}'
    ON CONFLICT (batch_id, record_key) DO NOTHING;
    """
    _run_sql_via_jdbc(spark, args, sql)


def main() -> None:
    parser = argparse.ArgumentParser(description="SDP Spark Processor (Phase 4) - Updated")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--processing-type", default="risk_scoring")
    parser.add_argument("--model-version", default="spark_demo_v1")

    # JDBC connection info
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-password", required=True)
    parser.add_argument("--db-driver", default="org.postgresql.Driver")

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"sdp-batch-{args.batch_id}")
        .getOrCreate()
    )

    try:
        # ---------------------------------------------------------
        # 0) Ensure staging infra exists
        # ---------------------------------------------------------
        _ensure_staging_table(spark, args)
        _clear_staging_for_batch(spark, args)

        # ---------------------------------------------------------
        # 1) Validate batch exists + tenant ownership
        # ---------------------------------------------------------
        batch_df = _read_jdbc_query(
            spark,
            args,
            f"""
            SELECT batch_id, client_id, processing_type, status
            FROM processing_batches
            WHERE batch_id = '{args.batch_id}'
            """,
        )

        if batch_df.rdd.isEmpty():
            raise RuntimeError(f"Batch not found: {args.batch_id}")

        batch = batch_df.collect()[0]

        if batch["client_id"] != args.client_id:
            raise RuntimeError(
                f"Forbidden: batch belongs to client_id={batch['client_id']}"
            )

        # Optional processing_type check
        if batch["processing_type"] != args.processing_type:
            raise RuntimeError(
                f"Batch processing_type mismatch: db={batch['processing_type']} cli={args.processing_type}"
            )

        # ---------------------------------------------------------
        # 2) Read tokenized records
        # ---------------------------------------------------------
        tokenized_df = _read_jdbc_query(
            spark,
            args,
            f"""
            SELECT batch_id, record_key, payload
            FROM tokenized_records
            WHERE batch_id = '{args.batch_id}'
            """,
        )

        if tokenized_df.rdd.isEmpty():
            print("No tokenized records found; nothing to do.")
            return

        # ---------------------------------------------------------
        # 3) Idempotency: remove already-processed records
        # ---------------------------------------------------------
        existing_df = _read_jdbc_query(
            spark,
            args,
            f"""
            SELECT batch_id::text AS batch_id, record_key
            FROM processed_results
            WHERE batch_id = '{args.batch_id}'
            """,
        )

        to_process_df = tokenized_df.join(
            existing_df,
            on=["batch_id", "record_key"],
            how="left_anti",
        )

        if to_process_df.rdd.isEmpty():
            print("All records already processed for this batch; nothing to do.")
            return

        # ---------------------------------------------------------
        # 4) Feature extraction
        # NOTE: payload is assumed to be a struct with field "email"
        # If payload is JSON string in your DB, you must parse it first.
        # ---------------------------------------------------------
        df = to_process_df.select(
            col("batch_id"),
            col("record_key"),
            col("payload.email").alias("email_token"),
        )

        # ---------------------------------------------------------
        # 5) Demo scoring logic
        # ---------------------------------------------------------
        scored_df = (
            df.withColumn("email_token", trim(col("email_token")))
            .withColumn(
                "risk_score",
                least(length(col("email_token")), lit(99)).cast("string"),
            )
            .withColumn("model_version", lit(args.model_version).cast("string"))
        )

        # IMPORTANT: Write TEXT into staging
        stage_df = scored_df.select(
            col("batch_id").cast("string").alias("batch_id"),
            col("record_key").cast("string").alias("record_key"),
            col("risk_score").cast("string").alias("risk_score"),
            col("model_version").cast("string").alias("model_version"),
        )

        # ---------------------------------------------------------
        # 6) Write stage results (safe types)
        # ---------------------------------------------------------
        _write_jdbc_table(stage_df, args, STAGING_TABLE, mode="append")

        # ---------------------------------------------------------
        # 7) Move from staging -> final with UUID cast and ON CONFLICT
        # ---------------------------------------------------------
        _insert_into_final_from_staging(spark, args)

        print("Spark processing complete.")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
