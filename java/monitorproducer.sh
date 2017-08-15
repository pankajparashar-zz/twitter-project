#!/bin/sh
while [ true ]
do
	val=$(df /home/ubuntu/ --output=pcent | grep -o '[0-9]*')
	
	if [ $val -gt 40 ]
	then
		scp -i "aws_firstdb.pem" -r *.data "ubuntu@ec2-52-39-10-10.us-west-2.compute.amazonaws.com:/data/twtr/"
		rm /home/ubuntu/*.data
	fi
		
	pfiles=$(ps -e | grep 'stream')
	#echo ${#pfiles[@]}
	if [ ${#pfiles[@]} -gt 1 ]
	then
		echo "working"
		sleep 900
	else
		nohup /home/ubuntu/stream.sh &
	fi
done
