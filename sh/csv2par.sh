#!/bin/bash
DT=$1

PY_PATH='~/airflow/par/mk_par.py'
PAR_PATH='~/data/parq/$DT'
mkdir -p $FILE_PATH
python $PATH '$DT'
