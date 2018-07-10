FROM python:2.17.15

ARG BRANCH=frontend6
ARG HOME=/app

WORKDIR $HOME
RUN mkdir -p /etc/dpkg/dpkg.cfg.d \
    &&  echo "path-exclude=/usr/share/locale/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  echo "path-exclude=/usr/share/man/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  echo "path-exclude=/usr/share/doc/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  apt-get -qq update \
    &&  apt-get -y install --no-install-recommends \
        build-essential \
        libffi-dev \
        libssl-dev \
        curl \
        git \
        vim-tiny \
        nano \
        sudo \
    && rm -rf /var/lib/apt/lists/* /usr/share/doc/* /usr/share/man/* /usr/share/locale/* \
    && git clone https://github.com/mozilla/ActiveData.git $HOME \
    && git checkout $BRANCH \
    && git config --global user.email "klahnakoski@mozilla.com" \
    && git config --global user.name "Kyle Lahnakoski"
    && mkdir $HOME/logs
