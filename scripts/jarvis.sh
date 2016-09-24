#!/bin/bash

# Load environment variables
source /home/ubuntu/run.sh

HOME_DIR=/home/ubuntu/cs838-p1
WORKLOAD_DIR=/home/ubuntu/workload/hive-tpcds-tpch-workload

# Clean directories
rm -rf $HOME_DIR/results/part-1

declare -a queries=(12 21 50 71 85)
declare -a runs=(01 02 03 04 05)
declare -a engines=(mr tez)

for q in "${queries[@]}"
do
  for r in "${runs[@]}"
  do
     
    for e in "${engines[@]}"
    do 
      WORKING_DIR=$HOME_DIR/results/part-1/query$q/$e/$r
      mkdir -p $WORKING_DIR
    
      # Pre query
      bash $HOME_DIR/scripts/pre-query-run.sh $HOME_DIR $WORKING_DIR $e
    
      # Run query
      bash $WORKLOAD_DIR/run_query_hive_$e.sh $q > $WORKING_DIR/query.out 2>&1

      # Post query
      bash $HOME_DIR/scripts/post-query-run.sh $HOME_DIR $WORKING_DIR $e
      java -cp $HOME_DIR/scripts DiskNetworkStatCalculator $WORKING_DIR/pre-stats.out $WORKING_DIR/post-stats.out > $WORKING_DIR/stats.out
    done
  done
done 
