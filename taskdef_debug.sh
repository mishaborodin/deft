#!/usr/bin/env bash

base_path=/data/prodsys

cd $base_path/deftcore
/usr/local/bin/python3.7 manage.py debug -t $1
