import logging
import sys
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Literal, NamedTuple

import polars as pl

from polars_data_quality_demo.ingestion import get_dataframe_count

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


class DataQualityResult(NamedTuple):
    rejected_data_table_path: str
    rejected_data_table_count: int
    valid_data_table_path: str
    valid_data_table_count: int
    date: datetime


def write_to_delta_table(
    *,  # all the parameters after this one are keyword-only
    path: str,
    mode: Literal["append", "overwrite"],
    quality_checks: list[pl.Expr],
) -> Callable:
    def decorator(f: Callable[[Any], pl.DataFrame]):
        @wraps(f)
        def wrapper(*args, **kwargs):
            now = datetime.now()
            now_iso = now.timestamp()
            rejected_path = f"{path}_rejected_{now_iso}"
            human_readable_quality_checks = [str(e) for e in quality_checks]

            result: pl.DataFrame = f(*args, **kwargs)

            if not isinstance(
                result, pl.DataFrame
            ):  # make sure the result of callable is a polars.DataFrame
                raise ValueError(
                    f"Expected a polars.DataFrame, but got {type(result)} instead.",
                )

            formatted_quality_checks = ", ".join(human_readable_quality_checks)
            logger.info(
                f"Running quality checks [{formatted_quality_checks}] on data",
            )

            quality_check_df = result.with_columns(
                pl.when(quality_checks)
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("__internal__valid"),
            )  # add a column to the DataFrame with the results of the quality checks

            valid_df = quality_check_df.filter(
                pl.col("__internal__valid") == "true",
            ).drop(
                "__internal__valid"
            )  # filter the DataFrame to keep only the valid rows

            valid_df.write_delta(path, mode=mode)

            logger.info(f"Written valid data to {path}")

            rejected_df = (
                quality_check_df.filter(pl.col("__internal__valid") == "false")
                .with_columns(
                    pl.lit(now).alias("__internal__timestamp"),
                    pl.lit(human_readable_quality_checks).alias(
                        "__internal__conditions",
                    ),  # add a column with the timestamp and the quality checks that were defined  # noqa: E501
                )
                .drop("__internal__valid")
            )

            rejected_df.write_delta(
                rejected_path,
                mode="overwrite",
            )

            logger.info(f"Written rejected data to {rejected_path}")

            data_quality_result = DataQualityResult(
                rejected_data_table_path=rejected_path,
                rejected_data_table_count=get_dataframe_count(rejected_df),
                valid_data_table_path=path,
                valid_data_table_count=get_dataframe_count(valid_df),
                date=now,
            )

            logger.info(
                f"Quality checks completed at {now}, results: {data_quality_result}",
            )

            return data_quality_result

        return wrapper

    return decorator
