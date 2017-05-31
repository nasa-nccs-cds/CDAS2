#!/usr/bin/env bash

source activate cdas
python -m pycdas.worker $*

