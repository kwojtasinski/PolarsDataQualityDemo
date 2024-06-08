ARG PACKAGE_VERSION
FROM polars_data_quality_demo:dev as development
RUN poetry build
WORKDIR /app
FROM python:3.11.9-slim as prod
RUN groupadd --gid 1000 dev && useradd --uid 1000 --gid 1000 -m dev
USER dev
WORKDIR /app
COPY --from=development --chown=dev:dev /app/dist /app/dist/
RUN WHEEL_FILE_PATH=`find /app/dist -iname *.whl | tac | head -n 1` && pip install $WHEEL_FILE_PATH && rm -rf /app/dist
CMD [ "/bin/bash" ]
