from datetime import date

import polars as pl

from polars_data_quality_demo.ingestion import get_dataframe_count, load_people_data


def test_load_people_data() -> None:
    df = load_people_data(
        start_date=date(year=2024, month=1, day=1),
        end_date=date(year=2024, month=12, day=31),
        size=1000,
        add_nulls=True,
    )
    assert df.height == 1000
    assert df.width == 5
    assert df.columns == [
        "name",
        "address",
        "email",
        "date_of_birth",
        "date_of_registration",
    ]
    assert get_dataframe_count(df.filter(pl.col("name").is_null())) == 1
    assert get_dataframe_count(df.filter(pl.col("address").is_null())) == 1
    assert get_dataframe_count(df.filter(pl.col("email").is_null())) == 1
    assert get_dataframe_count(df) == 1000
