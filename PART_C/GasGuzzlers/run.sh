#!/bin/bash
python $1 -r hadoop --output-dir $3 --no-output hdfs://andromeda.eecs.qmul.ac.uk$2
