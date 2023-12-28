import pandas as pd
import json
import os
import altair as alt
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import wasserstein_distance
# sns.set(style='ticks', context='paper', font='CMU Sans Serif')
sns.set(style='ticks', context='paper')
from collections import defaultdict
import numpy as np
import subprocess

# test 63 with 20M flows
time_limit=int(3000 * 1e9)
save_path="/data2/lichenni/ns3"
for mix_id in range(192):
    mix_dir = f'../data/{mix_id}'
    
    file_sldn=f'{save_path}/sldn_{mix_id}.npy'

    # if not os.path.exists(file_sldn):
    if True:
        file=f'{mix_dir}/ns3/fct_topology_flows_dctcp.txt'
        cmd = (
            "cat %s" % (file)
            + " | awk '{if ($5==100 && $7+$8<"
            + "%d" % time_limit
            + ") {slow=$8/$9;print slow<1?$9:$8, $9, $6, $7, $2, $3, $1}}' | sort -n -k 4"
        )
        # print cmd
        output = subprocess.check_output(cmd, shell=True, text=True)
        a = output.split("\n")[:-1]
        n = len(a)
        res_np = np.array([x.split() for x in a])   
        fcts = res_np[:, 0].astype("int64")
        i_fcts = res_np[:, 1].astype("int64")
        flow_sizes = res_np[:, 2].astype("int64")
        id = res_np[:, -1].astype("int64")
        sldn=   np.divide(fcts, i_fcts)
        id_to_slnd_size={}
        for i in range(len(sldn)):
            id_to_slnd_size[id[i]]=[sldn[i],flow_sizes[i]]
        np.save(file_sldn, id_to_slnd_size)
    else:
        id_to_slnd_size=np.load(file_sldn,allow_pickle=True).item()
    print(f"id_to_slnd_size: {len(id_to_slnd_size)}")

    file_path_to_flowId=f'{save_path}/path_to_flowId_{mix_id}.npz'

    if not os.path.exists(file_path_to_flowId):
        path_to_info={}
        path_to_flowid={}
        n_path_sampled=0
        with open(f'{mix_dir}/mlsys/path.txt', 'r') as file:
            for line_idx,line in enumerate(file):
                if line_idx==0:
                    n_path_sampled=int(line.strip().split(",")[1])
                elif line_idx==1:
                    path_list = line.strip()[:-1].split(",")
                    for path in path_list:
                        parts=path.split(":")
                        path_to_info[parts[0]]=[int(parts[1]),int(parts[2])]
                else:
                    # Remove leading and trailing whitespaces
                    parts = line.strip().split(':')

                    # Extract the ranges part (before ':') and the numbers part (after ':')
                    path, numbers_part = parts[0], parts[1]

                    flowid_list = [int(x) for x in numbers_part.split(',')]
                    
                    path_to_flowid[path]=flowid_list
        np.savez(file_path_to_flowId, n_path_sampled=n_path_sampled,path_to_info=path_to_info,path_to_flowid=path_to_flowid)
    else:
        data=np.load(file_path_to_flowId, allow_pickle=True)
        n_path_sampled=data['n_path_sampled']
        path_to_info=data['path_to_info'].item()
        path_to_flowid=data['path_to_flowid'].item()
                
    assert n_path_sampled==len(path_to_info)

    n_flows_in_sampled_paths=np.sum([path_to_info[key][1] for key in path_to_info])
    print(f"n_flows_in_sampled_paths: {n_flows_in_sampled_paths}")