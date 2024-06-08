# ruff: noqa: E501
from datetime import datetime

import polars as pl

from polars_data_quality_demo.ingestion import (
    load_people_data,  # that's the name of our Python (poetry) package
)

people_data = load_people_data(
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2021, 1, 1),
    size=1000,
    add_nulls=True,
)


people_data.write_delta("/tmp/people_data_delta")  # write the data to a Delta table
people_data = pl.read_delta(
    "/tmp/people_data_delta"
)  # read the data from the Delta table
print(people_data)  # print the data
missing_name_data = people_data.filter(pl.col("name").is_null())  # filter the data
print(missing_name_data)  # print the filtered data
# Output:
# shape: (1_000, 5)
# ┌─────────────────────┬──────────────────────────────┬──────────────────────────┬───────────────┬────────────────────────────┐
# │ name                ┆ address                      ┆ email                    ┆ date_of_birth ┆ date_of_registration       │
# │ ---                 ┆ ---                          ┆ ---                      ┆ ---           ┆ ---                        │
# │ str                 ┆ str                          ┆ str                      ┆ date          ┆ datetime[μs]               │
# ╞═════════════════════╪══════════════════════════════╪══════════════════════════╪═══════════════╪════════════════════════════╡
# │ null                ┆ 241 Foster Greens            ┆ hcardenas@example.net    ┆ 1997-02-17    ┆ 2020-07-24 09:26:49.001762 │
# │                     ┆ East Laura, …                ┆                          ┆               ┆                            │
# │ Desiree Guzman      ┆ null                         ┆ williamssean@example.net ┆ 1965-12-03    ┆ 2020-03-24 13:12:38.461160 │
# │ Christopher Sanchez ┆ 553 Rachel Alley             ┆ null                     ┆ 1995-03-31    ┆ 2020-11-13 16:14:12.439619 │
# │                     ┆ New Eric, NC …               ┆                          ┆               ┆                            │
# │ Daniel Little       ┆ 4261 Samuel Drive            ┆ vanessa19@example.net    ┆ null          ┆ 2020-09-06 18:55:35.683532 │
# │                     ┆ West Melanie…                ┆                          ┆               ┆                            │
# │ Eric James          ┆ 06569 Pena Isle Suite 932    ┆ timothy18@example.com    ┆ 1965-03-21    ┆ null                       │
# │                     ┆ Jens…                        ┆                          ┆               ┆                            │
# │ …                   ┆ …                            ┆ …                        ┆ …             ┆ …                          │
# │ Jennifer Dodson     ┆ 63858 Sean Heights           ┆ lori17@example.com       ┆ 1963-11-17    ┆ 2020-08-30 10:07:00.458311 │
# │                     ┆ Christopher…                 ┆                          ┆               ┆                            │
# │ Tanya Johnson       ┆ 18259 Hayden Spring          ┆ nicole97@example.net     ┆ 1974-10-05    ┆ 2020-10-14 16:48:54.240655 │
# │                     ┆ North Kris…                  ┆                          ┆               ┆                            │
# │ Marissa May         ┆ 1488 Rodriguez Keys Apt. 164 ┆ imoore@example.com       ┆ 1973-06-06    ┆ 2020-04-28 01:34:31.077078 │
# │                     ┆ P…                           ┆                          ┆               ┆                            │
# │ Richard Tran        ┆ 728 Morrow Course Apt. 068   ┆ rpowers@example.net      ┆ 2002-10-22    ┆ 2020-08-28 05:18:47.981541 │
# │                     ┆ Ran…                         ┆                          ┆               ┆                            │
# │ Shelby Martinez     ┆ 563 Mejia Pike               ┆ jeffrey49@example.com    ┆ 1960-05-09    ┆ 2020-08-31 22:01:40.492012 │
# │                     ┆ New Shannon, MO…             ┆                          ┆               ┆                            │
# └─────────────────────┴──────────────────────────────┴──────────────────────────┴───────────────┴────────────────────────────┘
