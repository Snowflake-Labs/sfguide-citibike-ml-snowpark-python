FROM continuumio/miniconda3:4.10.3

COPY environment.yml .
COPY ../include/snowflake_snowpark_python-0.4.0-py3-none-any.whl /tmp
RUN conda update conda && \
    conda env create -f environment.yml #&& \
