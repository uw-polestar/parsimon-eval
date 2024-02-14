import os
import json
from collections import defaultdict

cc_dict={
    "dctcp":"dctcp",
    "timely": "timely_vwin",
    "dcqcn": "dcqcn_paper_vwin",
    "hp": "hp",
}
def find_large_files(json_file):
    large_files = []
    with open(json_file, 'r') as f:
        data = json.load(f)
        
    file_list=[]
    for item_idx, item in enumerate(data):
        cc=item['cc']
        param_cc=round(item['param_cc'],1)
        file_name=f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{item_idx}/ns3-config/fct_topology_flows_{cc_dict[cc]}_k{item['window']}_b1.0_p{param_cc}.txt"
        file_list.append(file_name)
    cc_cnt_dict=defaultdict(lambda:0)
    for item_idx, file_path in enumerate(file_list):
        # print(file_path)
        try:
            cc=data[item_idx]['cc']

            if os.path.exists(f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{item_idx}/ns3-config/elapsed.txt"):
                cc_cnt_dict[cc]+=1
            else:
                file_size = os.path.getsize(file_path)
            
                # Convert bytes to megabytes
                file_size_in_mb = file_size / (1024 * 1024)
            
                large_files.append((item_idx, file_size_in_mb))
                    
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
    print(cc_cnt_dict)
    return large_files

if __name__ == "__main__":

    json_file = 'all_config.mix.json'  # Replace with your JSON file path
    
    # Find large files
    large_files = find_large_files(json_file)
    print(f"{len(large_files)} large files found")
    for item_idx, file_size_in_mb in large_files:
        print(f"{item_idx} ({file_size_in_mb} MB)")
