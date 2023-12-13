#!/bin/bash
# sleep 6h
cur_dir=`pwd`
OUTPUT_PATH='/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data'
# OUTPUT_PATH="/data2/lichenni/path/input"

# cd $OUTPUT_PATH
for shard in {0..191..1}
# for shard in 0
do
    COMMAND="rm -rf ${OUTPUT_PATH}/${shard}/flowsim"
    echo "$COMMAND">>"$cur_dir/rm.log"
    ${COMMAND}>>"$cur_dir/rm.log"
    echo "">>"$cur_dir/rm.log"
done
