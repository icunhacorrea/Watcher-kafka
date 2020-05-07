#!/bin/bash

javac -cp .:../jars/*  ./main/*.java -Xlint
jar -cmf MANIFEST.MF WatcherKafka.jar ./main/*.class
rm ./main/*.class

