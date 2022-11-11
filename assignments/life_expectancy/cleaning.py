"""
Functions to cleaning data
"""
from typing import List
import time
import logging as log
import argparse
import pandas as pd
import numpy as np
from life_expectancy.utils import f_input_configs, import_csv_to_pd, \
    save_pd_to_csv, f_remove_non_digits
log.getLogger().setLevel(log.INFO)


def clean_data(location_filter: str = 'PT') -> None:
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
        representations_of_nan, location_col = f_input_configs(configs_path)

    log.info("Starting...")

    log.info("~~~~~~~~~ Extract Data ~~~~~~~~~")

    df_input = import_csv_to_pd(
        load_data_path, sep=',| \t|\t'
    )

    log.info("~~~~~~~~~ Transform Data ~~~~~~~~~")
    log.info("Transforming Input Data")

    df_input = df_input.rename(
        columns=rename_raw_cols
    ).query(    # filter location to processing only required data
        f'{location_col} == "{location_filter}"'
    )

    log.info("Unpivot Data")

    df_ = unpivot_for_digit_cols(
        df_input, unpivot_col_id_vars
    ).replace(
        representations_of_nan, np.nan
    ).dropna().astype(  # types of variable and value cols
        types_unpivot_cols
    )
    # it should
    df_["value"] = df_["value"]/10

    df_ = df_.rename(  # rename variable and value columns
        columns=rename_unpivot_cols
    )
    log.info("~~~~~~~~~ Load Data ~~~~~~~~~")
    save_pd_to_csv(df_, save_data_path)

    log.info("~~~~~~~~~ Finished ~~~~~~~~~")
    log.info('Execution time: %s', round(time.time() - t_start, 3))


def unpivot_for_digit_cols(
        df: pd.DataFrame,  # pylint: disable=invalid-name
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


def parse_args(args) -> argparse.Namespace:
    """
    This function has been created for parsing arguments,
    so that one can include this in unit testing

    :param args: args to be inputed
    :return: parsed args
    """
    parser = argparse.ArgumentParser(description='Running Assignement 1')
    parser.add_argument('-c', '--country', type=str,
                        help='country for filtering', required=False)
    return parser.parse_args(args)


if __name__ == '__main__':  # pragma: no cover

    import sys

    args_ = parse_args(sys.argv[1:])

    clean_data(args_.country)
