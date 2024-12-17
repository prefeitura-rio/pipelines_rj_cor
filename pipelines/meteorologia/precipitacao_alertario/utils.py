# -*- coding: utf-8 -*-
"""
Utils for precipitacao_alertario
"""
from typing import List, Tuple
from datetime import datetime
import numpy as np
import pandas as pd
from pipelines.utils.utils import log


def parse_date_columns_old_api(
    dataframe: pd.DataFrame, partition_date_column: str
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Parses the date columns to the partition format. Reformatado para manter o formato utilizado
    quando os dados foram salvos pela primeira vez (ano, mes, dia).
    """
    ano_col = "ano"
    mes_col = "mes"
    data_col = "dia"
    cols = [ano_col, mes_col, data_col]
    for col in cols:
        if col in dataframe.columns:
            raise ValueError(f"Column {col} already exists, please review your model.")

    dataframe[partition_date_column] = dataframe[partition_date_column].astype(str)
    dataframe[data_col] = pd.to_datetime(dataframe[partition_date_column], errors="coerce")

    dataframe[ano_col] = (
        dataframe[data_col].dt.year.fillna(-1).astype(int).astype(str).replace("-1", np.nan)
    )

    dataframe[mes_col] = (
        dataframe[data_col].dt.month.fillna(-1).astype(int).astype(str).replace("-1", np.nan)
    )

    dataframe[data_col] = (
        dataframe[data_col].dt.day.fillna(-1).astype(int).astype(str).replace("-1", np.nan)
    )

    return dataframe, [ano_col, mes_col, data_col]


def parse_date_columns(
    dataframe: pd.DataFrame, partition_date_column: str
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Parses the date columns to the partition format. Reformatado para manter o formato utilizado
    quando os dados foram salvos pela primeira vez (ano, mes, dia).
    """
    ano_col = "ano"
    mes_col = "mes"
    data_col = "dia"
    cols = [ano_col, mes_col, data_col]
    for col in cols:
        if col in dataframe.columns:
            raise ValueError(f"Column {col} already exists, please review your model.")

    dataframe[partition_date_column] = dataframe[partition_date_column].astype(str)
    dataframe[data_col] = pd.to_datetime(dataframe[partition_date_column], errors="coerce")

    dataframe[ano_col] = (
        dataframe[data_col].dt.year.fillna(-1).astype(int).astype(str).replace("-1", np.nan)
    )

    dataframe[mes_col] = (
        dataframe[data_col].dt.month.fillna(-1).astype(int).astype(str).replace("-1", np.nan)
    )

    dataframe[data_col] = (
        dataframe[data_col].dt.day.fillna(-1).astype(int).astype(str).replace("-1", np.nan)
    )

    return dataframe, [ano_col, mes_col, data_col]


def treat_date_col(row):
    """
    Add zero hour if date cames without it
    """
    if len(row) == 10:
        row = row + " 00:00:00"
    return row


def replace_future_dates(dfr: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces future dates in the 'data_medicao' column with the current date if they are in the
    future. This is done to ensure that the data is not projected into the future, but rather
    reflects the current state.
    """
    current_datetime_sp = datetime.now()
    current_date_sp = datetime.now().date()
    dfr_future = dfr[dfr["data_medicao"] > current_datetime_sp].copy()

    if dfr_future.shape[0] > 0:
        for i in range(dfr_future.shape[0]):
            log(f"\nFuture data found on API:\n{dfr_future.iloc[i]}\n")

        dfr["data_medicao"] = dfr["data_medicao"].apply(
            lambda x: (
                x.replace(
                    year=current_date_sp.year, month=current_date_sp.month, day=current_date_sp.day
                )
                if x.date() > current_date_sp
                else x
            )
        )
    return dfr


def remove_future_dates(dfr: pd.DataFrame) -> pd.DataFrame:
    """
    Remove future dates in the 'data_medicao' column with the current date if they are in the
    future. This is done to ensure that the data is not projected into the future.
    """
    current_datetime_sp = datetime.now()
    current_date_sp = datetime.now().date()
    dfr_future = dfr[dfr["data_medicao"] > current_datetime_sp].copy()

    if dfr_future.shape[0] > 0:
        for i in range(dfr_future.shape[0]):
            log(f"\nFuture data found on API:\n{dfr_future.iloc[i]}\n")

        dfr = dfr[dfr["data_medicao"] <= current_date_sp]
    return dfr
