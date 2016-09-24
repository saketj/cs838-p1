#!/bin/sh

#Set the env vars
source /home/ubuntu/run.sh

#Remove log directories
if [ $3 == "tez" ]; then
  hadoop fs -rm -r /tmp/tez-history/*
  rm -rf /mnt/logs/apps/application_*
fi

if [ $3 == "mr" ]; then
  hadoop fs -rm -r /tmp/hadoop-yarn/staging/history/done/*
fi

#Drop caches
ssh ubuntu@vm1 'echo abcde12345 | sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh ubuntu@vm2 'echo abcde12345 | sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh ubuntu@vm3 'echo abcde12345 | sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh ubuntu@vm4 'echo abcde12345 | sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'

#Compute network and disk stats
bash $1/scripts/get-disk-networks-stats.sh $2/pre-stats.out 
