import os
import numpy as np

save_path="/data2/lichenni/ns3"
# for mix_id in range(1):
for mix_id in [1]:
    mix_dir = f'../data/{mix_id}'

    file_path_to_flowId=f'{save_path}/path_to_flowId_{mix_id}_fig7.npz'

    if not os.path.exists(file_path_to_flowId):
        path_to_flowid={}
        with open(f'{mix_dir}/mlsys/path_to_flows.txt', 'r') as file:
            for line_idx,line in enumerate(file):
                parts = line.strip().split(':')

                # Extract the ranges part (before ':') and the numbers part (after ':')
                path, numbers_part = parts[0], parts[1]

                flowid_list = [int(x) for x in numbers_part.split(',')]
                
                path_to_flowid[path]=flowid_list
        np.savez(file_path_to_flowId, path_to_flowid=path_to_flowid)
    else:
        data=np.load(file_path_to_flowId, allow_pickle=True)
        path_to_flowid=data['path_to_flowid'].item()
    num_flows_path = [len(path_to_flowid[path]) for path in path_to_flowid]
    print(f"mix_id: {mix_id}, path_to_flowid: {len(path_to_flowid)}")            