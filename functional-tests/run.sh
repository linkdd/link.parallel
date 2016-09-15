#!/bin/bash

function failure() {
    ipcluster stop || exit 2
    exit 1
}

ipcluster start -n 4 --daemonize || exit 1
sleep 10

aloe features/mapreduce.feature || failure

ipcluster stop || exit 2
