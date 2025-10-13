#!/bin/bash
conda activate raft
source .venv/bin/activate
# python3 waf configure build --enable-raft-test
# time build/deptran_server -f config/raft_lab_test.yml 2>&1 | tee output.txt
python3 waf configure build --enable-raft-test --debug
time gdb --args build/deptran_server -f config/raft_lab_test.yml 2>&1 | tee output-dbg.txt