# syntax=docker/dockerfile:1.7

FROM python:3.14-slim-trixie

WORKDIR /app

# qbit-src is a named build context (`--build-context qbit-src=...`), not a stage.
# hadolint ignore=DL3022
COPY --from=qbit-src test/functional /opt/qbit-functional
COPY lab/auxpow/*.py /app/lab/auxpow/

ENV PYTHONPATH=/app:/opt/qbit-functional

CMD ["python3", "-m", "lab.auxpow.auxpow_coordinator"]
