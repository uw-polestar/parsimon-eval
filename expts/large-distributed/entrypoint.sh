#!/bin/bash
mode=${1}
# echo "Mode: $mode"
# eval "$mode"
if [ "$mode" == "parsimon-worker" ]; then
  eval "$mode"
elif [ "$mode" == "parsimon-manager" ]; then
  eval sh
else
  echo "Invalid ENTRYPOINT_MODE: $ENTRYPOINT_MODE"
  exit 1
fi