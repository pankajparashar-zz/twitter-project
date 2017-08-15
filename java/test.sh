#!/bin/bash

val=$(df /data2/ --output=pcent | grep -o '[0-9]*')
echo $val
if [[ $val -gt 6 ]]
then
	echo 'greater'
else
	echo 'not greater'
fi
