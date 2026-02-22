import time

def log_backfill_progress(query, poll_seconds: int = 2):
    """
    Logs per-batch backfill progress using Spark streaming metrics.
    Works with trigger(availableNow=True).
    """
    print("🚀 Backfill started")

    seen_batches = set()
    total_rows = 0

    while query.isActive:
        progress = query.lastProgress

        if progress:
            batch_id = progress.get("batchId")
            num_rows = progress.get("numInputRows", 0)

            # Only log each batch once
            if batch_id not in seen_batches:
                seen_batches.add(batch_id)
                total_rows += num_rows

                print(
                    f"📦 Batch {batch_id} processed {num_rows:,} rows "
                    f"(total: {total_rows:,})",
                    flush=True
                )

        time.sleep(poll_seconds)

    print("✅ Backfill complete (availableNow finished)")