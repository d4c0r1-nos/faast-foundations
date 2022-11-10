"""Pytest configuration file"""
import pandas as pd
import pytest
import yaml

from . import FIXTURES_DIR, OUTPUT_DIR, CONFIGS_DIR
from life_expectancy.utils import f_config_parser


@pytest.fixture(autouse=True)
def run_before_and_after_tests() -> None:
    """Fixture to execute commands before and after a test is run"""
    # Setup: fill with any logic you want

    yield # this is where the testing happens

    # Teardown : fill with any logic you want
    file_path = OUTPUT_DIR / "pt_life_expectancy.csv"
    file_path.unlink(missing_ok=True)


@pytest.fixture(scope="session")
def pt_life_expectancy_expected() -> pd.DataFrame:
    """Fixture to load the expected output of the cleaning script"""
    return pd.read_csv(FIXTURES_DIR / "pt_life_expectancy_expected.csv")


@pytest.fixture(scope="session")
def get_configs() -> dict:
    """Fixture to load the configs"""
    return f_config_parser(CONFIGS_DIR / "configs.yml")
