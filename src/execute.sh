#!/bin/bash

if [ $1 == "logger" ]; then
	java -cp .:jars/kafka-clients-2.3.1.jar:jars/slf4j-api-1.7.28.jar:jars/logback-classic-1.2.3.jar:jars/logback-core-1.2.3.jar:jars/recordutil.jar \
		main.WatcherKafka $2 $3 $4
elif [ $1 == "nlogger" ]; then
	java -cp .:jars/kafka-clients-2.3.1.jar:jars/slf4j-api-1.7.28.jar:jars/recordutil.jar \
		main.WatcherKafka $2 $3 $4
fi

