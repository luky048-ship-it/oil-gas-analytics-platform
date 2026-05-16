from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class MartConfig:
    table_name: str
    critical_columns: List[str]
    unique_key: List[str]
    business_rules: Dict[str, Any]

# Глобальные параметры алгоритмов
ANALYSIS_PARAMS = {
    "z_score_threshold": 3.0,
    "risk_rolling_window": 7 * 24 * 60,  # 7 дней в минутах
    "kpi_rolling_window": 7,             # 7 дней для волл-кпи
}

# Спецификации контрактов для каждой витрины
MART_CONTRACTS = {
    "mart_production": MartConfig(
        table_name="gold.mart_production",
        critical_columns=["well_id", "date"],
        unique_key=["well_id", "date"],
        business_rules={"min_oil_ton": 0}
    ),
    "mart_well_kpi": MartConfig(
        table_name="gold.mart_well_kpi",
        critical_columns=["well_id", "date"],
        unique_key=["well_id", "date"],
        business_rules={}
    ),
    "mart_failures": MartConfig(
        table_name="gold.mart_failures",
        critical_columns=["pump_id", "date", "timestamp"],
        unique_key=["pump_id", "timestamp"],
        business_rules={"max_z_score": 10.0}
    ),
    "mart_logistics": MartConfig(
        table_name="gold.mart_logistics",
        critical_columns=["delivery_id", "date"],
        unique_key=["delivery_id"],
        business_rules={}
    )
}
