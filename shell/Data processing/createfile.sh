#!/bin/bash
int_n=0
int_y=0
total_n_file=100
total_y_file=300
echo "start create total n files"
rm files
mkdir files
while(($int_n<$total_n_file))
do
	echo $int_n > "files/file_"$int_n"_N"
	
	let "int_n++"
	echo "create file "$int_n
done
echo "start create total y files"
while(($int_y<$total_y_file))
do
	echo $int_y >  "files/file_"$int_y"_Y"
	let "int_y++"
	echo "create file "$int_y
done
