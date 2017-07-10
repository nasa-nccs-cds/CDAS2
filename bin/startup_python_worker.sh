#!/usr/bin/env bash

source $HOME/.cdas/sbin/setup_runtime.sh
source activate cdas2
python -m pycdas.worker $*

