import os
import json
from collections import defaultdict

cc_dict={
    "dctcp":"dctcp",
    "timely": "timely_vwin",
    "dcqcn": "dcqcn_paper_vwin",
}
def find_large_files(json_file, json_file_param, size_limit,save_file):
    large_files = []
    with open(json_file, 'r') as f:
        data = json.load(f)
    with open(json_file_param, 'r') as f:
        data_param = json.load(f)
        
    file_list=[]
    
    for item_idx, item in enumerate(data):
        
        item_param=data_param[item_idx]
        cc=item_param['cc']
        file_name=f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{item_idx}/ns3-param/fct_topology_flows_{cc_dict[cc]}_k{item_param['window']}.txt"
        file_list.append(file_name)
    
    new_json=[]
    new_json_param=[]
    
    new_json_mlsys=[]
    new_json_param_mlsys=[]
    
    new_json_mlsys_done=[]
    new_json_param_mlsys_done=[]
    
    new_json_pmn_m=[]
    new_json_param_pmn_m=[]
    
    cc_cnt_dict=defaultdict(lambda:0)
    window_dict=defaultdict(lambda:0)
    cnt_running=0
    for item_idx, file_path in enumerate(file_list):
        # print(file_path)
        try:
            cc=data_param[item_idx]['cc']
            # if cc=="dctcp": continue
            # Get the size of the file in bytes
            file_size = os.path.getsize(file_path)
            
            # Convert bytes to megabytes
            file_size_in_mb = file_size / (1024 * 1024)

            if os.path.exists(f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{item_idx}/mlsys-param/elapsed.txt"):
                new_json_mlsys_done.append(data[item_idx])
                new_json_param_mlsys_done.append(data_param[item_idx])
                cc_cnt_dict[cc]+=1
                
                if cc=="dctcp":
                    new_json_pmn_m.append(data[item_idx])
                    new_json_param_pmn_m.append(data_param[item_idx])
            else:
                if file_size_in_mb > size_limit:
                    large_files.append((file_path, file_size_in_mb))
                    
                    if not os.path.exists(f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{item_idx}/ns3-param/records.csv"):
                        new_json.append(data[item_idx])
                        new_json_param.append(data_param[item_idx])
                    else:
                        cc_cnt_dict[cc]+=1
                        if cc=="dctcp":
                            window_dict[data_param[item_idx]['window']]+=1

                            if cc=="dctcp":
                                new_json_pmn_m.append(data[item_idx])
                                new_json_param_pmn_m.append(data_param[item_idx])
                        new_json_mlsys.append(data[item_idx])
                        new_json_param_mlsys.append(data_param[item_idx])
                        
                        new_json_mlsys_done.append(data[item_idx])
                        new_json_param_mlsys_done.append(data_param[item_idx])
                else:
                    if cc=="dctcp":
                        print(f"{cnt_running}-{item_idx}: File size: {file_size_in_mb} MB")
                        cnt_running+=1
        
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
    print(f"ns3-param-running: {cnt_running}, ns3-param: {len(new_json)}, mlsys: {len(new_json_mlsys)}, mlsys-done: {len(new_json_mlsys_done)}")
    print(cc_cnt_dict)
    print(window_dict)
    
    with open(f"{save_file}.mix.json", 'w') as f:
        json.dump(new_json, f, indent=2)
    with open(f"{save_file}_param.mix.json", 'w') as f:
        json.dump(new_json_param, f, indent=2)
        
    with open(f"{save_file}_mlsys.mix.json", 'w') as f:
        json.dump(new_json_mlsys, f, indent=2)
    with open(f"{save_file}_mlsys_param.mix.json", 'w') as f:
        json.dump(new_json_param_mlsys, f, indent=2)
    
    with open(f"mlsys.mix.json", 'w') as f:
        json.dump(new_json_mlsys_done, f, indent=2)
    with open(f"mlsys_param.mix.json", 'w') as f:
        json.dump(new_json_param_mlsys_done, f, indent=2)
    
    with open(f"pmn_m.mix.json", 'w') as f:
        json.dump(new_json_pmn_m, f, indent=2)
    with open(f"pmn_m_param.mix.json", 'w') as f:
        json.dump(new_json_param_pmn_m, f, indent=2)
    
    return large_files

if __name__ == "__main__":
    size_limit_mb = 600

    json_file = 'all.mix.json'  # Replace with your JSON file path
    json_file_param = 'all_param_window.mix.json'  # Replace with your JSON file path
    save_file = 'test'  # Replace with your JSON file path
    
    # Find large files
    large_files = find_large_files(json_file, json_file_param,size_limit_mb,save_file)
    
    # for file_path, file_size_in_mb in large_files:
    #     print(f"{file_path} ({file_size_in_mb} MB)")
