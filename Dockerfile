FROM fletchjeff/vhol-citibike-ml:v0.2

# Change this Dockerfile to the code below if you want to build your own image
#
# FROM quay.io/astronomer/astro-runtime:5.0.2
# USER root
# RUN apt-get -y update && apt-get -y upgrade
# RUN apt-get install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev wget -y
# RUN wget https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tar.xz
# RUN tar -xf Python-3.8.12.tar.xz
# RUN mv Python-3.8.12 /opt/Python3.8.12
# WORKDIR /opt/Python3.8.12/
# RUN ./configure  
# #--enable-optimizations --enable-shared
# RUN make
# RUN make altinstall
# RUN ldconfig /opt/Python3.8.12
# COPY include/snowflake_snowpark_python-0.6.0-py3-none-any.whl /tmp
# RUN pip3.8 install '/tmp/snowflake_snowpark_python-0.6.0-py3-none-any.whl[pandas]'
# USER astro
# WORKDIR /usr/local/airflow