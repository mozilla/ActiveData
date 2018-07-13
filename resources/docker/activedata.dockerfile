FROM python:2.7

ARG REPO_TAG=
ARG REPO_URL=https://github.com/mozilla/ActiveData
ARG REPO_BRANCH=dev
ARG BUILD_URL=https://travis-ci.org/mozilla/ActiveData
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
    && if [ -z ${REPO_TAG+x}]; then git checkout tags/$REPO_TAG; else git checkout $REPO_BRANCH; fi \
    && git config --global user.email "klahnakoski@mozilla.com" \
    && git config --global user.name "Kyle Lahnakoski" \
    && mkdir $HOME/logs

RUN python -m pip --no-cache-dir install --user -r requirements.txt \
    && python -m pip install gunicorn

RUN export PYTHONPATH=.:vendor \
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

CMD /usr/local/bin/gunicorn -b 0.0.0.0:$PORT --config=resources/docker/gunicorn.py active_data.app:flask_app
