#Run the following first from a terminal command-line...
#DOCKER_BUILDKIT=0 docker build --build-arg PYTHON_MAJOR_MINOR_VERSION=3.8 --build-arg VERSION=2.2.5-2 -t ap-airflow:py38 https://github.com/astronomer/ap-airflow.git#master:2.2.5/bullseye

FROM ap-airflow:py38
USER root
RUN apt-get update && apt-get install --reinstall -y build-essential \
	&& apt-get install -y gcc 
#\
#	&& apt-get install -y wget \
#	&& rm -rf /var/lib/apt/lists/*
#
#ENV PATH="/root/miniconda3/bin:${PATH}"
#ARG PATH="/root/miniconda3/bin:${PATH}"
#RUN wget \
#    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
#    && mkdir /root/.conda \
#    && bash Miniconda3-latest-Linux-x86_64.sh -b \
#    && rm -f Miniconda3-latest-Linux-x86_64.sh
#
#RUN conda install -c https://repo.anaconda.com/pkgs/snowflake snowflake_snowpark_python==0.7.0[pandas]
#RUN pip install snowflake_snowpark_python==0.7.0[pandas]

COPY include/snowflake_snowpark_python-0.6.0-py3-none-any.whl /tmp
RUN pip install '/tmp/snowflake_snowpark_python-0.6.0-py3-none-any.whl[pandas]'
