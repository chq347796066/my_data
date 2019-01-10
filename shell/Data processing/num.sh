#!/bin/bash
function rand(){  
        min=$1  
        max=$(($2-$min+1))  
        num=$(cat /proc/sys/kernel/random/uuid | cksum | awk -F ' ' '{print $1}')  
        echo $(($num%$max+$min))  
    }  
      
    rnd=$(rand 100 500)  
    echo $rnd  
	
exit 0
