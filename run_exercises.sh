#!/bin/bash

mvn package
case "$1" in
  "1") $FLINK_HOME/flink run -c es.upm.master.exercise1 target/LinearRoadProject-1.0-SNAPSHOT.jar -input vehiclesData.csv -output out_exercise1.csv
  ;;
  "2") $FLINK_HOME/flink run -c es.upm.master.exercise2 target/LinearRoadProject-1.0-SNAPSHOT.jar -input vehiclesData.csv -output out_exercise2.csv -speed 45 -time 30 -startSegment 45 -endSegment 60
	;;
  "3") $FLINK_HOME/flink run -c es.upm.master.exercise3 target/LinearRoadProject-1.0-SNAPSHOT.jar -input vehiclesData.csv -output1 out_exercise3-1.csv -output2 out_exercise3-2.csv -segment 47
	;;
  *) echo "Usage: $0 1|2|3"
  ;;
esac

