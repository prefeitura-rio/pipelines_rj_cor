# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Schedules for setting rain dashboard using radar data.
"""
from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

TIME_SCHEDULE = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 0, 30),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults={
                # "trigger_rain_dashboard_update": False,
                "materialize_after_dump": False,
                "mode": "prod",
                "materialize_to_datario": False,
                "dump_to_gcs": False,
                "dump_mode": "append",
                "save_image_without_background": True,
                "save_image_with_background": False,
                "save_image_without_colorbar": True,
                "save_image_with_colorbar": True,
                "preprocessing_gypscie": True,
                "workflow_id": 40,
                "environment_id": 1,
                "domain_id": 1,
                "project_id": 1,
                "project_name": "rionowcast_precipitation",
                "processor_name": "etl_inea_radar",
                "dataset_processor_id": 43,
                "load_data_function_id": 46,
                "filter_data_function_id": 47,
                "parse_date_time_function_id": 48,
                "aggregate_data_function_id": 49,
                "save_data_function_id": 50,
                "dataset_id_previsao_chuva": "clima_previsao_chuva",
                "table_id_previsao_chuva": "preprocessamento_radar_mendanha",
                "station_type": "radar",
                "source": "mendanha",
                "model_version": 1,
            },
        ),
    ]
)
