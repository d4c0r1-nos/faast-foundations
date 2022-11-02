"""
Functions to cleaning data
"""
import pandas as pd
import numpy as np
from typing import List
import time
from life_expectancy.utils import f_input_configs, import_csv_to_pd, \
    save_pd_to_csv, f_remove_non_digits
import logging as log
log.getLogger().setLevel(log.INFO)


def clean_data() -> None:
    # load_data_path: str,
    # save_data_path: str,
    # rename_raw_cols: Dict[str, str],
    # unpivot_col_id_vars: List[str],
    # rename_unpivot_cols: Dict[str, str],
    # types_unpivot_cols: Dict[str, type],
    # representations_of_nan: List[str],
    # location_col: str,
    # location_filter: str
    """
    Main Script to clean the data.

    It extracts data from .csv, transforms and saves to a new .csv

    Currently it only supports filtering by one region at a time.

    :return: None
    """

    t_start = time.time()

    # hardcoded because this func cannot support input
    configs_path = 'life_expectancy/configs/configs.yml'

    # for inputs description, check f_input_configs docstring
    load_data_path, save_data_path, rename_raw_cols, \
        unpivot_col_id_vars, rename_unpivot_cols, types_unpivot_cols, \
        representations_of_nan, location_col, \
        location_filter = f_input_configs(configs_path)

    log.info("Starting...")

    log.info("~~~~~~~~~ Extract Data ~~~~~~~~~")

    df_input = import_csv_to_pd(
        load_data_path, sep=',|\t'
    )

    log.info("~~~~~~~~~ Transform Data ~~~~~~~~~")

    log.info("Transforming Input Data")

    df_input = df_input.rename(
        columns=rename_raw_cols
    ).query(    # filter location to processing only required data
        '{} == "{}"'.format(
            location_col, location_filter
        )
    )

    log.info("Unpivot Data")

    df_ = unpivot_for_digit_cols(
        df_input, unpivot_col_id_vars
    ).replace(
        representations_of_nan, np.nan
    ).dropna().astype(  # types of variable and value cols
        types_unpivot_cols
    ).rename(  # rename variable and value columns
        columns=rename_unpivot_cols
    )

    log.info("~~~~~~~~~ Load Data ~~~~~~~~~")
    save_pd_to_csv(df_, save_data_path)

    log.info("~~~~~~~~~ Finished ~~~~~~~~~")
    log.info('Execution time: {}s'.format(round(time.time() - t_start, 3)))


def unpivot_for_digit_cols(
        df: pd.DataFrame,
        col_id_vars: List[str]
) -> pd.DataFrame:
    """
    This function unpivot a list of columns to long.

    :param df: Pandas Dataframe to unpivot
    :param col_id_vars: list of columns id_vars
    :return: Unpivot Pandas Dataframe
    """

    df = df.melt(id_vars=col_id_vars)

    # remove digits
    df['variable'], df['value'] = \
        f_remove_non_digits(df['variable']), \
        f_remove_non_digits(df['value'])

    # rename and types could have been done in previous
    return df[col_id_vars + ['variable', 'value']]


if __name__ == '__main__':

    # hardcoded path | to be replaced
    # configs_path = 'configs/configs.yml'
    # configs_path = 'life_expectancy/configs/configs.yml'

    # configs = f_config_parser(configs_path)

    clean_data()
    #     configs['load_data_path'], configs['save_data_path'],
    #     # if no dictionary is provided to rename,
    #     # it assumes no renaming => {}
    #     configs.get('rename_raw_columns', {}), configs['col_id_vars'], configs.get('rename_columns', {}),
    #     configs.get('ensure_col_types', {}), configs['representations_of_nan'],
    #     configs.get('col_location', 'region'), configs.get('location_filter', 'PT')
    #
