FROM python:3.8.5

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py \
    | POETRY_HOME=/opt/poetry python && \
        cd /usr/local/bin && \
        ln -s /opt/poetry/bin/poetry && \
        poetry config virtualenvs.create false

# Copy using poetry.lock* in case it doesn't exist yet
COPY ./app/pyproject.toml ./app/poetry.lock* /app/
WORKDIR /app
# RUN poetry install --no-root --no-dev
RUN poetry install --no-root

COPY ./app /app
