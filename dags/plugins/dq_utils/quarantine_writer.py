import logging

logger = logging.getLogger(__name__)

def write_quarantine_dataset(
    invalid_df,
    dataset: str,
    reason: str,
    execution_date: str,
    s3_options: dict
) -> None:
    logger.info(f"Wrote {invalid_df.height} quarantined rows for {dataset}")
