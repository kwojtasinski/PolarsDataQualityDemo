format:
	poetry run ruff format .
	poetry run ruff check . --fix
test:
	poetry run pytest .
lint:
	poetry run ruff check .
build:
	poetry build
	docker build -f dev.Dockerfile -t "polars_data_quality_demo:dev" .
	docker build -f Dockerfile -t "polars_data_quality_demo:latest" .