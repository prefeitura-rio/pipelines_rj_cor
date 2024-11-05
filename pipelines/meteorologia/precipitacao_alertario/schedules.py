# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for precipitacao_alertario
Rodar a cada 1 minuto
"""
from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

minute_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=2),
            start_date=datetime(2021, 1, 1, 0, 1, 0),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "trigger_rain_dashboard_update": True,
                "materialize_after_dump_old_api": True,
                "materialize_to_datario_old_api": True,
                "materialize_after_dump": True,
                "materialize_to_datario": False,
                "mode": "prod",
                "dump_to_gcs": False,
                "maximum_bytes_processed": None,
                "preprocessing_gypscie": True,
                "workflow_id": 41,
                "environment_id": 1,
                "domain_id": 1,
                "project_id": 1,
                "project_name": "rionowcast_precipitation",
                "treatment_version": 1,
                "processor_name": "etl_alertario22",
                "dataset_processor_id": 43,
                "load_data_function_id": 53,
                "parse_date_time_function_id": 54,
                "drop_duplicates_function_id": 55,
                "replace_inconsistent_values_function_id": 56,
                "add_lat_lon_function_id": 57,
                "save_data_function_id": 58,
                "rain_gauge_metadata_path": 227,
                "dataset_id_previsao_chuva": "clima_previsao_chuva",
                "table_id_previsao_chuva": "preprocessamento_pluviometro_alertario",
                "station_type": "rain_gauge",
                "source": "alertario",
                "model_version": 1,
            },
        ),
    ]
)
