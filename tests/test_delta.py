from datetime import date
from pathlib import Path

import polars as pl
import pytest

from polars_data_quality_demo.delta import write_to_delta_table
from polars_data_quality_demo.ingestion import get_dataframe_count, load_people_data


def test_write_to_delta_table_with_valid_function(tmpdir) -> None:
    quality_checks = [pl.col("name").is_not_null(), pl.col("email").is_not_null()]
    decorator = write_to_delta_table(
        path=Path(tmpdir / "test").as_posix(),
        mode="overwrite",
        quality_checks=quality_checks,
    )(load_people_data)

    results = decorator(
        start_date=date(year=2024, month=1, day=1),
        end_date=date(year=2024, month=12, day=31),
        size=1000,
        add_nulls=True,
    )

    rejected = pl.read_delta(results.rejected_data_table_path)
    valid = pl.read_delta(results.valid_data_table_path)

    assert get_dataframe_count(rejected.filter(pl.col("name").is_null())) == 1
    assert get_dataframe_count(rejected.filter(pl.col("email").is_null())) == 1
    assert get_dataframe_count(valid) == results.valid_data_table_count


def test_write_to_delta_table_without_valid_function(tmpdir) -> None:
    decorator = write_to_delta_table(
        path=Path(tmpdir / "test").as_posix(),
        mode="overwrite",
        quality_checks=[],
    )(print)
    with pytest.raises(ValueError, match="Expected a polars.DataFrame"):
        decorator()
