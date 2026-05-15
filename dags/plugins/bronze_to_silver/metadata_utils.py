# plugins/bronze_to_silver/metadata_utils.py
from datetime import datetime
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)

def get_last_watermark(dataset: str) -> Optional[datetime]:
    """
    Mocked for testing. Returns None to process all partitions.
    """
    return None

def update_pipeline_watermark(dataset: str, watermark: datetime, execution_date: str) -> None:
    """
    Mocked for testing.
    """
    logger.info(f"Updated watermark for {dataset} to {watermark}")

def publish_pipeline_metadata(result: Any) -> None:
    """
    Mocked for testing.
    """
    logger.info(f"Published metadata for {result.dataset}")
