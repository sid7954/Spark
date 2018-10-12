#!/bin/bash

spark-submit --master $1 --driver-memory 32G  --executor-memory 32G  --executor-cores 10 --conf $2 part2.py $3 $4