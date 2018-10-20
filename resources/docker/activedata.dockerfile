FROM python:2.7

ARG BUILD_URL=
ARG REPO_CHECKOUT=
ARG REPO_URL=https://github.com/mozilla/ActiveData
ARG HOME=/app
ARG USER=app

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
        supervisor \
    && rm -rf /var/lib/apt/lists/* /usr/share/doc/* /usr/share/man/* /usr/share/locale/* \
    && git clone $REPO_URL.git $HOME \
    && git checkout $REPO_CHECKOUT \
    && mkdir $HOME/logs \
    && export PYTHONPATH=.:vendor \
    && python -m pip --no-cache-dir install --user -r requirements.txt \
    && python -m pip install gunicorn \
    && python resources/docker/version.py

RUN addgroup --gid 10001 $USER \
    && adduser \
       --gid 10001 \
       --uid 10001 \
       --home $HOME \
       --shell /usr/sbin/nologin \
       --no-create-home \
       --disabled-password \
       --gecos we,dont,care,yeah \
       $USER

CMD export PYTHONPATH=.:vendor \
    && /usr/local/bin/gunicorn -b 0.0.0.0:$PORT --config=resources/docker/gunicorn.py active_data.app:flask_app
