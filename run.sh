#!/bin/bash
conda activate raft
source .venv/bin/activate
python3 waf configure build --enable-raft-test
build/deptran_server -f config/raft_lab_test.yml