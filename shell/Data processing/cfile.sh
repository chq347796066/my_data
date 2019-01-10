#!/bin/bash
folder="./data"
rm -rf y_files n_files
mkdir y_files
mkdir n_files
for file in  `ls $folder`
do
	echo $file
	if [[ $file =~ "Y" ]];then 
		mv $folder"/"$file  y_files
		echo "yes"
	else
		mv $folder"/"$file n_files
		echo "no"
	fi
done
