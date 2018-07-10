FROM python:2.7

ARG BRANCH=dev
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
    && git config --global user.name "Kyle Lahnakoski" \
    && mkdir $HOME/logs

RUN python -m pip --no-cache-dir install --user -r requirements.txt \
    && python -m pip install gunicorn \
    && python -m pip install pyopenssl \
    && python -m pip install ndg-httpsclient \
    && python -m pip install pyasn1 \
    && python -m pip install supervisor

CMD /usr/local/bin/supervisord -c $HOME/resources/docker/supervisord.conf
