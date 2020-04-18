#!/bin/bash
python $1 -r hadoop --file outputTop10.txt --output-dir $3 --no-output hdfs://andromeda.eecs.qmul.ac.uk$2
