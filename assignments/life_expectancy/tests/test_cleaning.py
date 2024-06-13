"""Tests for the cleaning module"""
import pandas as pd
import numpy as np

from life_expectancy.cleaning import main, parse_args
from . import OUTPUT_DIR


def test_clean_data(pt_life_expectancy_expected):
    """Run the `clean_data` function and compare the output to the expected output"""
    main()
    pt_life_expectancy_actual = pd.read_csv(
        OUTPUT_DIR / "pt_life_expectancy.csv"
    )
    pd.testing.assert_frame_equal(
        pt_life_expectancy_actual, pt_life_expectancy_expected
    )


def test_location_filter(get_configs):
    """Run the `clean_data` function and compare the output to the expected output"""

    main('ES')

    location_col = get_configs.get('col_location', 'region')

    # did not change the path, but we should
    es_filter = np.unique(pd.read_csv(
        OUTPUT_DIR / "pt_life_expectancy.csv"
    )[location_col].to_numpy())

    nok_msg = "clean_data location filtering is not working"
    assert len(es_filter) == 1, nok_msg
    assert es_filter[0] == 'ES', nok_msg


def test_args_parser():
    """Test args parsing"""

    parser = parse_args(['-c', 'ES'])

    assert parser.country == 'ES', 'country arg parsing has been changed'
