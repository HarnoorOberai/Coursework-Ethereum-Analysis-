#!/bin/bash
python $1 -r hadoop hdfs://andromeda.eecs.qmul.ac.uk/user/hso30/outPart_B_job1 --output-dir $3 --no-output hdfs://andromeda.eecs.qmul.ac.uk$2
