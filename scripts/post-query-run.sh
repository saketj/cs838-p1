#Generate stats after the query completes
bash $1/scripts/get-disk-networks-stats.sh $2/post-stats.out

#Copy logs and dot files
mkdir -p $2/logs
if [ $3 == "tez" ]; then
  hadoop fs -get /tmp/tez-history/* $2/logs/
  ssh ubuntu@vm1 'find /mnt/logs -name "*.dot"' | xargs -I file scp ubuntu@vm1:file $2/logs/
  ssh ubuntu@vm2 'find /mnt/logs -name "*.dot"' | xargs -I file scp ubuntu@vm2:file $2/logs/
  ssh ubuntu@vm3 'find /mnt/logs -name "*.dot"' | xargs -I file scp ubuntu@vm3:file $2/logs/
  ssh ubuntu@vm4 'find /mnt/logs -name "*.dot"' | xargs -I file scp ubuntu@vm4:file $2/logs/
fi

if [ $3 == "mr" ]; then
  hadoop fs -get /tmp/hadoop-yarn/staging/history/done/*/*/*/*/*.jhist $2/logs/
fi
