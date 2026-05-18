from dataclasses import dataclass
from datetime import datetime


@dataclass
class PipelineExecutionResult:
    """Структура данных для хранения метрик и результатов выполнения пайплайна обработки датасета."""
    dataset: str
    partition_date: str
    processed_rows: int
    quarantined_rows: int
    output_path: str
    execution_time_sec: float
    watermark: datetime
