-- Временные таблицы для атомарной загрузки в Gold
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_mart_production (LIKE gold.mart_production INCLUDING ALL);
CREATE TABLE IF NOT EXISTS staging.stg_mart_well_kpi (LIKE gold.mart_well_kpi INCLUDING ALL);
CREATE TABLE IF NOT EXISTS staging.stg_mart_failures (LIKE gold.mart_failures INCLUDING ALL);
CREATE TABLE IF NOT EXISTS staging.stg_mart_logistics (LIKE gold.mart_logistics INCLUDING ALL);
