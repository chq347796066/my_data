#!/bin/bash
function rand(){  
        min=$1  
        max=$(($2-$min+1))  
        num=$(cat /proc/sys/kernel/random/uuid | cksum | awk -F ' ' '{print $1}')  
        echo $(($num%$max+$min))  
} 

out_num=$(rand 0 $files_length)
arr=(10 5 8 6 9 15 28 40 41 45 48 60)
get=6
int=0
while((int<$get))
do
	b=true
	echo $b
	while $b
	do
		index=$(rand 0 6)
		echo "index:"$index
		if echo "${s_arr[@]}" | grep -w $index &>/dev/null; then
    			b=true
		else
			b=false
			s_arr[int]=$index
		fi
	done
	let "int++"
done
s_arr_l=${#s_arr[@]}
echo "length:"$s_arr_l
for x in ${s_arr[@]}
do
	echo "x:"$x
done
