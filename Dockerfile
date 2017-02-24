FROM debian:8.6
MAINTAINER Kyle Wilcox <kyle@axiomdatascience.com>
ENV DEBIAN_FRONTEND noninteractive
ENV LANG C.UTF-8

RUN apt-get update && apt-get install -y \
        binutils \
        build-essential \
        bzip2 \
        ca-certificates \
        curl \
        libglib2.0-0 \
        libsm6 \
        libxext6 \
        libxrender1 \
        wget \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /var/cache/oracle-jdk8-installer

# Setup CONDA (https://hub.docker.com/r/continuumio/miniconda3/~/dockerfile/)
ENV MINICONDA_VERSION 4.2.12
RUN echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh && \
    curl -k -o /miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-$MINICONDA_VERSION-Linux-x86_64.sh && \
    /bin/bash /miniconda.sh -b -p /opt/conda && \
    rm /miniconda.sh && \
    /opt/conda/bin/conda config \
        --set always_yes yes \
        --set changeps1 no \
        --set show_channel_urls True \
        && \
    /opt/conda/bin/conda config \
        --add channels conda-forge \
        --add channels axiom-data-science \
        && \
    /opt/conda/bin/conda clean -a -y
ENV PATH /opt/conda/bin:$PATH

# Install python requirements
COPY requirements*.txt /tmp/

RUN conda install -y \
        --file /tmp/requirements.txt \
        --file /tmp/requirements-test.txt \
        && \
    conda clean -a -y

# Copy packrat contents and install
ENV CODE_HOME /beaker
WORKDIR $CODE_HOME
COPY . $CODE_HOME

CMD ["py.test", "-s", "-rxs", "-v"]
