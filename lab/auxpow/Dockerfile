# syntax=docker/dockerfile:1.7

FROM python:3.12-slim

WORKDIR /app

COPY --from=qbit-src test/functional /opt/qbit-functional
COPY lab/auxpow/*.py /app/lab/auxpow/

ENV PYTHONPATH=/app:/opt/qbit-functional

CMD ["python3", "-m", "lab.auxpow.auxpow_coordinator"]
