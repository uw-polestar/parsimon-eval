#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 ip_file"
    exit 1
fi

ip_file=$1
workers=""

while IFS= read -r ip
do
  workers+=" -w ${ip}:2727"
done < "$ip_file"

command="parsimon-manager --mix spec/mix.json ${workers} pmn-mc"
eval $command
