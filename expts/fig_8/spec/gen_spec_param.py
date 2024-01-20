import json
import numpy as np

np.random.seed(0)

cc_list=['dctcp','timely','dcqcn']
dctcp_k_list=[7, 13, 21, 29, 37, 45, 54, 60, 66, 70]

def add_param_json(json_file, output_file):
    # Read the JSON file
    with open(json_file, 'r') as f:
        data = json.load(f)
    n_mixes=len(data)
    
    cc_candidates=np.random.choice(cc_list, n_mixes, replace=True)
    dctcp_k_candidates=np.random.choice(dctcp_k_list, n_mixes, replace=True)
    
    data_new=[]
    for i in range(n_mixes):
        data_tmp={
            'cc': cc_candidates[i],
            'dctcp_k': int(dctcp_k_candidates[i]),
        }
        if cc_candidates[i]!='dctcp':
            data_tmp['dctcp_k']=30
        data_new.append(data_tmp)
    with open(output_file, 'w') as f:
        json.dump(data_new, f, indent=2)

# Example usage
json_file_path = 'all.mix.json'  # Replace with your JSON file path

output_file_path = 'all_param.mix.json'  # Replace with the desired output file path

add_param_json(json_file_path, output_file_path)
# sub_json(json_file_path, json_file_path_sub,output_file_path)
