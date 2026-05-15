import logging

logger = logging.getLogger(__name__)

def publish_pipeline_status(
    dataset: str,
    execution_date: str,
    status: str,
) -> None:
    logger.info(f"Published status {status} for {dataset} on {execution_date}")
