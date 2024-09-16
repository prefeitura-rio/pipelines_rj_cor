# -*- coding: utf-8 -*-
"""
Tasks for cor
"""
from typing import List

from prefect import task


@task(checkpoint=False)
def get_fake_data(index) -> List:
    """
    Returns the dataframe with the alerts.
    """
    data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

    return data[index]


@task(checkpoint=False)
def treat_fake_data(data: List) -> List:
    """
    Returns the dataframe with the alerts.
    """
    return [x * 10 for x in data]


@task(checkpoint=False)
def print_data(data: List) -> None:
    """
    Returns the dataframe with the alerts.
    """
    print(data)
    return None
