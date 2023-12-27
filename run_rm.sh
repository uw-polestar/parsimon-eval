#!/bin/bash
# sleep 6h
cur_dir=`pwd`
OUTPUT_PATH='/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data'
# OUTPUT_PATH="/data2/lichenni/path/input"

# cd $OUTPUT_PATH
for shard in {0..191..1}
# for shard in 0
do
    # COMMAND="rm -rf ${OUTPUT_PATH}/${shard}/mlsys"
    # COMMAND="mv ${OUTPUT_PATH}/${shard}/mlsys ${OUTPUT_PATH}/${shard}/mlsys_s1_bt10"
    COMMAND="mv ${OUTPUT_PATH}/${shard}/mlsys_s2_bt10 /data2/lichenni/data_10m/${shard}/"
    # COMMAND="mkdir /data2/lichenni/data_10m/${shard}"
    # COMMAND="rm ${OUTPUT_PATH}/${shard}/ns3-path-one/flows.txt"
    echo "$COMMAND">>"$cur_dir/rm.log"
    ${COMMAND}>>"$cur_dir/rm.log"
    echo "">>"$cur_dir/rm.log"
done
