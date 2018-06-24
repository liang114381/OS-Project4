#!/usr/bin/env bash

function int_handler
{
    for pid in ${pids[*]}
    do
        kill -9 $pid
    done
}

num=3
pids=()
trap int_handler SIGINT SIGTERM
for i in `seq 1 $num`
do
    python main.py --id=$i &
    pids+=($!)
done

wait

