#!/bin/bash

case "$1" in
  "start") $FLINK_HOME/start-cluster.sh
  ;;
  "stop") $FLINK_HOME/stop-cluster.sh
	;;
  *) echo "Usage: $0 start|stop"
  ;;
esac

