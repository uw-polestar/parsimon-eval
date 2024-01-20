import json
import os
import numpy as np

np.random.seed(0)

cc_list=['dctcp','timely_vwin','dcqcn_paper_vwin']
dctcp_k_list=[7, 13, 21, 29, 37, 45, 54, 60, 66, 70]

def add_param_json(json_file, output_file):
    # Read the JSON file
    with open(json_file, 'r') as f:
        data = json.load(f)
    n_mixes=len(data)
    
    cc_candidates=np.random.choice(cc_list, n_mixes, replace=True)
    dctcp_k_candidates=np.random.choice(dctcp_k_list, n_mixes, replace=True)
    
    for i in range(n_mixes):
        data[i]['cc']=cc_candidates[i]
        if cc_candidates[i]=='dctcp':
            data[i]['dctcp_k']=int(dctcp_k_candidates[i])
        else:
            data[i]['dctcp_k']=30
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

# Example usage
json_file_path = 'all.mix.json'  # Replace with your JSON file path

output_file_path = 'all_param.mix.json'  # Replace with the desired output file path

add_param_json(json_file_path, output_file_path)
# sub_json(json_file_path, json_file_path_sub,output_file_path)
