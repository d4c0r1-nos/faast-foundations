"""
General utils
"""
import re
import yaml
import pandas as pd


def f_config_parser(file_path: str) -> dict:
    """
    Parse configurations from a given .yaml configuration file
    :param: str
        Configuration file relative or absolute path
    :return: dict
        Dict with parameter names as keys and parameter values as values

    """

    return yaml.load(
        open(file_path, encoding="utf8"),
        Loader=yaml.Loader
    )


def f_input_configs(file_path: str) -> (str, ):
    """
    Since we cannot have input args in the clean() function,
    this replaces that 'feature'

    :param file_path: str

    :return: 9 variables from yml
            load_data_path: str
                The path to the data to be extracted
            save_data_path: str
                the path to data being saved
            rename_raw_cols: dict
                dict with key pairs {'old_name': 'new_name'}
                for renaming input columns
            unpivot_col_id_vars: list
                cols to be id variables when unpivoting
            rename_unpivot_cols: dict
                dict with key pairs {'old_name': 'new_name'}
                to rename value and variable columns after unpivot
            types_unpivot_cols: dict
                dict with key pairs {'old_name': type}
                gives the types of variable and value after unpivot
            representations_of_nan: list
                values to convert to NaN, as they represent NaN
            location_col: str
                "final" name (if renamed) of the region column, from input data
    """
    configs = f_config_parser(file_path)

    return configs['load_data_path'], configs['save_data_path'], \
        configs.get('rename_raw_columns', {}), configs['col_id_vars'], \
        configs.get('rename_columns', {}), configs.get('ensure_col_types', {}), \
        configs['representations_of_nan'], configs.get('col_location', 'region')


def import_csv_to_pd(path_csv: str, sep: str = ',') -> pd.DataFrame:
    """
    Import csv file to pd.Dataframe

    :param path_csv: str
        the path to the csv file

    :param sep: str, default = ,

        the character used to split columns

    :return: pd.Dataframe
        data
    """
    return pd.read_csv(path_csv, sep=sep, engine='python')


def save_pd_to_csv(
        df: pd.DataFrame,   # pylint: disable=invalid-name
        path_csv: str
) -> None:
    """
    Save dataframe to .csv.

    :param df: pandas dataframe that one wants to save
    :param path_csv: path to .csv file
    :return: None
    """
    df.to_csv(
        path_or_buf=path_csv,
        index=False
    )


def f_remove_non_digits(to_clean: pd.Series) -> pd.Series:
    """
    Function to help cleaning non digit numbers from pd columns.

    :param to_clean: pd.Series, column to clean
    :return: pd.Series, column cleaned
    """
    return to_clean.map(
            lambda s: re.sub(r'[^0-9]', '', s)
        )
