#!/usr/bin/env bash

git fetch
git pull

cd ${CDAS_HOME_DIR}
sbt publish
sbt stage
python setup.py install
