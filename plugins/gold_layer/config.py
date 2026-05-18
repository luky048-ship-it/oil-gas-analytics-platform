from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class MartConfig:
    """Конфигурационный класс для описания параметров витрины данных: связей с источниками и бизнес-правил."""
    table_name: str
    critical_columns: List[str]
    unique_key: List[str]
    business_rules: Dict[str, Any]
    source_datasets: Dict[str, List[str]]


# Параметры статистического анализа
ANALYSIS_PARAMS = {
    "z_score_threshold": 3.0,
    "risk_rolling_window": 7 * 24 * 60,
    "kpi_rolling_window": 7,
}

# Реестр конфигураций для всех витрин Gold-слоя
MART_CONTRACTS = {
    "mart_production": MartConfig(
        table_name="gold.mart_production",
        critical_columns=["well_id", "date"],
        unique_key=["well_id", "date"],
        business_rules={"min_oil_ton": 0},
        source_datasets={
            "production": ["well_id", "date"],
            "well_telemetry": ["well_id", "timestamp"],
            "well_targets": ["well_id", "date"],
        },
    ),
    "mart_well_kpi": MartConfig(
        table_name="gold.mart_well_kpi",
        critical_columns=["well_id", "date"],
        unique_key=["well_id", "date"],
        business_rules={},
        source_datasets={
            "production": ["well_id", "date"],
        },
    ),
    "mart_failures": MartConfig(
        table_name="gold.mart_failures",
        critical_columns=["pump_id", "date", "timestamp"],
        unique_key=["pump_id", "timestamp"],
        business_rules={"max_z_score": 10.0},
        source_datasets={
            "pump_sensors": ["pump_id", "timestamp"],
            "pump_failures": ["pump_id", "timestamp"],
            "pumps": ["pump_id"],
        },
    ),
    "mart_logistics": MartConfig(
        table_name="gold.mart_logistics",
        critical_columns=["delivery_id", "date"],
        unique_key=["delivery_id"],
        business_rules={},
        source_datasets={
            "deliveries": ["delivery_id"],
            "drivers": ["driver_id"],
            "vehicles": ["vehicle_id"],
        },
    ),
}
