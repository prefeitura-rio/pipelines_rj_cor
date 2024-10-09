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
                "environment_id": 1,
                "domain_id": 1,
                "project_id": 1,
                "project_name": "rionowcast_precipitation",
                "processor_name": "etl_alertario22",
                "dataset_processor_id": 43,
                "dataset_id_previsao_chuva": "clima_previsao_chuva",
                "table_id_previsao_chuva": "preprocessamento_pluviometro_alertario",
                "station_type": "pluviometro",
                "source": "alertario",
                "maximum_bytes_processed": None,
                "model_version": 1,
            },
        ),
    ]
)
