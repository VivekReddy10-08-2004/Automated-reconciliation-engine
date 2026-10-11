FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

ARG APP_UID=10001
ARG APP_GID=10001

RUN groupadd --gid "${APP_GID}" app \
    && useradd --uid "${APP_UID}" --gid app --create-home app

WORKDIR /app

COPY orchestration/docker/requirements-batch.txt /tmp/requirements-batch.txt
RUN python -m pip install --requirement /tmp/requirements-batch.txt

COPY --chown=app:app reconciliation_engine/ /app/reconciliation_engine/

RUN mkdir -p /app/state \
    && chown app:app /app/state

USER app

VOLUME ["/app/state"]

CMD ["python", "-m", "reconciliation_engine.batch", "--db", "/app/state/reconciliation.duckdb", "--data-root", "/app/state/data"]
