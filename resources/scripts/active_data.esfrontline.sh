#!/bin/bash
set -e

FRONTLINE_DIR="/home/ec2-user/esFrontLine"
FRONTLINE_ENV="/home/ec2-user/esFrontLine-env"
BRANCH=activedata

# Clone frontine
if [[ ! -d $FRONTLINE_DIR ]] ; then
	git clone https://github.com/mozilla/esFrontLine.git $FRONTLINE_DIR
fi

# Update to required branch
cd $FRONTLINE_DIR
git checkout $BRANCH
git pull origin $BRANCH

# Install requirements in virtualenv
if [[ ! -d $FRONTLINE_ENV ]] ; then
	virtualenv $FRONTLINE_ENV
fi
source ${FRONTLINE_ENV}/bin/activate
pip install -r ${FRONTLINE_DIR}/requirements.txt
