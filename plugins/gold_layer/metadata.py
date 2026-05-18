from dataclasses import dataclass
from datetime import datetime

@dataclass
class MartBuildResult:
    """Описывает результат построения витрины данных, включая метрики производительности и прогресса."""
    mart_name: str
    processed_rows: int
    inserted_rows: int
    execution_time_sec: float
    partition_date: str
    watermark: datetime
