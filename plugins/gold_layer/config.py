from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class MartConfig:
    table_name: str
    critical_columns: List[str]
    unique_key: List[str]
    business_rules: Dict[str, Any]
    source_datasets: Dict[str, List[str]]


ANALYSIS_PARAMS = {
    "z_score_threshold": 3.0,
    "risk_rolling_window": 7 * 24 * 60,
    "kpi_rolling_window": 7,
}

MART_CONTRACTS = {
    "mart_production": MartConfig(
        table_name="gold.mart_production",
        critical_columns=["well_id", "date"],
        unique_key=["well_id", "date"],
        business_rules={"min_oil_ton": 0},
        source_datasets={
            "production": ["prod_id"],
            "well_telemetry": ["record_id"],
            "well_targets": ["well_id", "date"],
        },
    ),
    "mart_well_kpi": MartConfig(
        table_name="gold.mart_well_kpi",
        critical_columns=["well_id", "date"],
        unique_key=["well_id", "date"],
        business_rules={},
        source_datasets={
            "production": ["prod_id"],
        },
    ),
    "mart_failures": MartConfig(
        table_name="gold.mart_failures",
        critical_columns=["pump_id", "date", "timestamp"],
        unique_key=["pump_id", "timestamp"],
        business_rules={"max_z_score": 10.0},
        source_datasets={
            "pump_sensors": ["record_id"],
            "pump_failures": ["failure_id"],
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
