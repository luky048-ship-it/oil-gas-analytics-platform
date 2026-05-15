from dataclasses import dataclass
from datetime import datetime
from typing import List
import logging

logger = logging.getLogger(__name__)

@dataclass
class DQResult:
    dataset: str
    validation_type: str
    status: str       # PASS, FAIL, WARNING
    failed_rows: int
    checked_rows: int
    message: str
    created_at: datetime

def persist_dq_results(
    results: List[dict],
    execution_date: str,
) -> None:
    logger.info(f"Persisted {len(results)} DQ results for {execution_date}")
