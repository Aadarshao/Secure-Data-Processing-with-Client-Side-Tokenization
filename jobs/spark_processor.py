import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, least, lit


def main() -> None:
    parser = argparse.ArgumentParser(description="SDP Spark Processor")
    parser.add_argument("--batch-id", required=True)

    # JDBC connection info
    parser.add_argument("--db-url", required=True)        # e.g. jdbc:postgresql://sdp_postgres:5432/sdp
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-password", required=True)
    parser.add_argument("--db-driver", default="org.postgresql.Driver")

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"sdp-batch-{args.batch_id}")
        .getOrCreate()
    )

    # -----------------------------
    # Read tokenized records for this batch
    # -----------------------------
    tokenized_df = (
        spark.read.format("jdbc")
        .option("url", args.db_url)
        .option(
            "query",
            f"""
            SELECT batch_id, record_key, payload
            FROM tokenized_records
            WHERE batch_id = '{args.batch_id}'
            """
        )
        .option("user", args.db_user)
        .option("password", args.db_password)
        .option("driver", args.db_driver)
        .load()
    )

    if tokenized_df.rdd.isEmpty():
        print("No tokenized records found for batch; exiting.")
        spark.stop()
        return

    # -----------------------------
    # Read already-processed results for this batch
    # (so reruns don't create duplicates)
    # -----------------------------
    existing_results_df = (
        spark.read.format("jdbc")
        .option("url", args.db_url)
        .option(
            "query",
            f"""
            SELECT batch_id, record_key
            FROM processed_results
            WHERE batch_id = '{args.batch_id}'
            """
        )
        .option("user", args.db_user)
        .option("password", args.db_password)
        .option("driver", args.db_driver)
        .load()
    )

    # -----------------------------
    # Keep only records that are NOT already processed
    # -----------------------------
    to_process_df = tokenized_df.join(
        existing_results_df,
        on=["batch_id", "record_key"],
        how="left_anti",
    )

    if to_process_df.rdd.isEmpty():
        print("All records already processed for this batch; nothing to do.")
        spark.stop()
        return

    # -----------------------------
    # Extract fields needed for scoring
    # payload is JSONB; Spark will treat it like a struct/map
    # -----------------------------
    df = to_process_df.select(
        col("batch_id"),
        col("record_key"),
        col("payload.email").alias("email_token"),
    )

    # -----------------------------
    # Demo scoring
    # risk_score = min(len(email_token), 99)
    # -----------------------------
    scored_df = (
        df.withColumn("risk_score", least(length(col("email_token")), lit(99)).cast("string"))
          .withColumn("model_version", lit("spark_demo_v1"))
    )

    output_df = scored_df.select(
        "batch_id",
        "record_key",
        "risk_score",
        "model_version",
    )

    # -----------------------------
    # Write new results only
    # -----------------------------
    (
        output_df.write.format("jdbc")
        .option("url", args.db_url)
        .option("dbtable", "processed_results")
        .option("user", args.db_user)
        .option("password", args.db_password)
        .option("driver", args.db_driver)
        .mode("append")
        .save()
    )

    print("Spark processing complete.")
    spark.stop()


if __name__ == "__main__":
    main()
