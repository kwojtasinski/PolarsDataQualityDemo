from datetime import datetime

import polars as pl
from faker import Faker


def get_dataframe_count(dataframe: pl.DataFrame) -> int:
    return dataframe.select(pl.len()).to_dicts()[0]["len"]


def load_people_data(
    start_date: datetime.date,
    end_date: datetime.date,
    size: int = 1000,
    add_nulls: bool = False,
) -> pl.DataFrame:
    """
    Function to generate a fake dataset of people data.
    :param start_date: The start date for the date_of_registration column.
    :param end_date: The end date for the date_of_registration column.
    :param size: The number of rows to generate.
    :param add_nulls: Add nulls to the dataset.
    """
    _faker = Faker()
    data = {
        "name": [_faker.name() for _ in range(size)],
        "address": [_faker.address() for _ in range(size)],
        "email": [_faker.email() for _ in range(size)],
        "date_of_birth": [
            _faker.date_of_birth(minimum_age=18, maximum_age=65) for _ in range(size)
        ],
        "date_of_registration": [
            _faker.date_time_between(start_date=start_date, end_date=end_date)
            for _ in range(size)
        ],
    }
    if add_nulls is True:
        data["name"][0] = None
        data["address"][1] = None
        data["email"][2] = None
        data["date_of_birth"][3] = None
        data["date_of_registration"][4] = None
    return pl.DataFrame(data)
