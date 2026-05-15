# plugins/bronze_to_silver/pipeline_execution.py
from dataclasses import dataclass
from datetime import datetime


@dataclass
class PipelineExecutionResult:
    dataset: str
    partition_date: str
    processed_rows: int
    quarantined_rows: int
    output_path: str
    execution_time_sec: float
    watermark: datetime
