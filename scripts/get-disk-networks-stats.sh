#!/bin/sh

OUTPUT=$1

#Getting the disk stats
ssh ubuntu@vm1 'cat /proc/diskstats' | tail -6  > $OUTPUT
ssh ubuntu@vm2 'cat /proc/diskstats' | tail -6  >> $OUTPUT
ssh ubuntu@vm3 'cat /proc/diskstats' | tail -6  >> $OUTPUT
ssh ubuntu@vm4 'cat /proc/diskstats' | tail -6  >> $OUTPUT

#Getting the network stats
ssh ubuntu@vm1 'cat /proc/net/dev' | tail -2 | head -1 >> $OUTPUT
ssh ubuntu@vm2 'cat /proc/net/dev' | tail -2 | head -1 >> $OUTPUT
ssh ubuntu@vm3 'cat /proc/net/dev' | tail -2 | head -1 >> $OUTPUT
ssh ubuntu@vm4 'cat /proc/net/dev' | tail -2 | head -1 >> $OUTPUT
