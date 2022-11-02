# If you are using an M1 mac, the docker build process takes ages. There is a 
# prebuilt image available to make this process go faster. To use the standard 
# build process, comment the line below and uncomment the rest of the document.

# FROM fletchjeff/vhol-citibike:v0.3

FROM quay.io/astronomer/astro-runtime:5.0.2
USER root
RUN apt-get -y update \
  && apt-get -y upgrade \
  && apt-get install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev wget -y \
  && wget https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tar.xz \
  && tar -xf Python-3.8.12.tar.xz \
  && mv Python-3.8.12 /opt/Python3.8.12
WORKDIR /opt/Python3.8.12/
RUN ./configure \
#--enable-optimizations --enable-shared
  && make \
  && make altinstall \
  && ldconfig /opt/Python3.8.12 \
  && pip3.8 install snowflake-snowpark-python[pandas]==0.11.0 jupyterlab
USER astro
WORKDIR /usr/local/airflow

