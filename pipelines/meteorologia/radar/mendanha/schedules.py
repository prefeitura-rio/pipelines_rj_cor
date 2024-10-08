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
                "save_image_without_background": True,
                "save_image_with_background": False,
                "save_image_without_colorbar": True,
                "save_image_with_colorbar": True,
            },
        ),
    ]
)
