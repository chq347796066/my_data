#!/bin/bash
start_time=`date --date='0 days ago' "+%Y-%m-%d %H:%M:%S"`
rm -rf n_file part1 part2 y_files
rm -rf data
mkdir data
cp data2/* data
echo "cfile"
./cfile.sh
echo "Sfile.sh"
./Sfile.sh
echo "Sfile1.sh"
./Sfile1.sh
finish_time=`date --date='0 days ago' "+%Y-%m-%d %H:%M:%S"`
duration=$(($(($(date +%s -d "$finish_time")-$(date +%s -d "$start_time")))))
echo "total time $duration"
