#!/bin/bash

set -eu

if [ "$MODE" == "worker" ]; then
  exec parsimon-worker
elif [ "$MODE" == "manager" ]; then
  exec /bin/sh
else
  echo "Invalid MODE: $MODE"
  exit 1
fi