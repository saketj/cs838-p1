#Generate stats after the query completes
bash $1/scripts/get-disk-networks-stats.sh $2/post-stats.out

#Copy logs
mkdir -p $2/logs
if [ $3 == "tez" ]; then
  hadoop fs -get /tmp/tez-history/* $2/logs/
  cp `find /mnt/logs/apps -name "*.dot"` $2/logs/
fi

if [ $3 == "mr" ]; then
  hadoop fs -get /tmp/hadoop-yarn/staging/history/done/*/*/*/*/*.jhist $2/logs/
fi
