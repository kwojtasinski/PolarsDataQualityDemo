ARG PYTHON_BASE_VERSION=3.11.9
FROM python:${PYTHON_BASE_VERSION}-slim
ARG POETRY_VERSION=1.8.2
RUN groupadd --gid 1000 dev && useradd --uid 1000 --gid 1000 -m dev
USER dev
WORKDIR /app
ENV PATH=$PATH:/home/dev/poetry/bin
RUN python -m venv /home/dev/poetry && \
    /home/dev/poetry/bin/pip install poetry==${POETRY_VERSION}
COPY --chown=dev:dev pyproject.toml poetry.lock ./
# workaround, so poetry dependencies can be cached
RUN mkdir polars_data_quality_demo && touch README.md && touch polars_data_quality_demo/__init__.py
RUN poetry --version && poetry install
COPY --chown=dev:dev . .
RUN poetry install
CMD [ "/bin/bash" ]
