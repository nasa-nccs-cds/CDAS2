#!/usr/bin/env bash

source $HOME/.cdas/sbin/setup_runtime.sh
source activate cdas
python -m pycdas.worker $*

