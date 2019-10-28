#!/usr/bin bash


CPU_CORES=$(lscpu -p=CORE,ONLINE | grep -c 'Y')

echo {0..130} | xargs -n 1 -P ${CPU_CORES} python ./univ_stats.py
