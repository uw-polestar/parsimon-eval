#!/bin/bash
# sleep 6h
cur_dir=`pwd`
OUTPUT_PATH='/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data'
# OUTPUT_PATH="/data2/lichenni/path/input"

# cd $OUTPUT_PATH
for shard in {0..191..1}
# for shard in 0
do
    # COMMAND="rm -rf /data2/lichenni/data_10m/${shard}/mlsys_s2_bt50"
    # COMMAND="mv ${OUTPUT_PATH}/${shard}/mlsys ${OUTPUT_PATH}/${shard}/mlsys_s2_bt50_2k"
    # COMMAND="mv ${OUTPUT_PATH}/${shard}/mlsys_s2_bt50_2k /data2/lichenni/data_10m/${shard}/"
    COMMAND="mv /data2/lichenni/data_10m/${shard}/mlsys_s2_bt50_fast /data2/lichenni/data_10m/${shard}/mlsys_s2_bt50"
    # COMMAND="mkdir /data2/lichenni/data_10m/${shard}"
    # COMMAND="rm ${OUTPUT_PATH}/${shard}/ns3-path-one/flows.txt"
    echo "$COMMAND">>"$cur_dir/rm.log"
    ${COMMAND}>>"$cur_dir/rm.log"
    echo "">>"$cur_dir/rm.log"
done
