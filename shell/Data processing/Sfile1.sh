#!/bin/bash
folder="./y_files"
files=`ls $folder`
j=0
for i in $files
do
	files_arr[j]=$i
	j=`expr $j + 1`
done
files_length=${#files_arr[@]}
#生成随机数
 function rand(){  
        min=$1  
        max=$(($2-$min+1))  
        num=$(cat /proc/sys/kernel/random/uuid | cksum | awk -F ' ' '{print $1}')  
        echo $(($num%$max+$min))  
    } 
echo "files_length:"$files_length
s_file_num_m=`expr $files_length \* 3`
s_file_num=`expr $s_file_num_m / 10`
echo "select_files:"$s_file_num
# 先随机生成一个指定范围的数字作为初始值
a=0
#获取随机产生的数组文件
while((a<$s_file_num))
do
	b=true
	while $b
	do
		out_num=$(rand 0 $files_length)
		if echo "${select_files[@]}" | grep -w $out_num &>/dev/null; then
    			b=true
		else
			b=false
			select_files[a]=$out_num
		fi
	done
	let "a++"
done
echo "select_files"${#select_files[@]}
select_files_length=${#select_files[@]}
int=0
while((int<$select_files_length))
do
	index=${select_files[int]}
	s_file=${files_arr[index]}
	mv $folder"/"$s_file  test_set
	let "int++"
done
other_files=`ls $folder`
for x in $other_files
do
	mv $folder"/"$x development_set
done


