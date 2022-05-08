#DOCKER_BUILDKIT=0 docker build --build-arg PYTHON_MAJOR_MINOR_VERSION=3.8 --build-arg VERSION=2.2.5-2 -t ap-airflow:py38 https://github.com/astronomer/ap-airflow.git#master:2.2.5/bullseye

FROM ap-airflow:py38
USER root
RUN apt-get update && apt-get install --reinstall -y build-essential &&  apt-get install -y gcc
COPY include/snowflake_snowpark_python-0.6.0-py3-none-any.whl /tmp
RUN pip install '/tmp/snowflake_snowpark_python-0.6.0-py3-none-any.whl[pandas]'
