FROM python:3.12-slim

RUN pip install duckdb jupyter pandas matplotlib

WORKDIR /notebooks
