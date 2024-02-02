import json
import os

def filter_json(json_file, json_file_param, output_file):
    # Read the JSON file
    with open(json_file, 'r') as f:
        data = json.load(f)
    with open(json_file_param, 'r') as f:
        data_param = json.load(f)
    
    remain_data = []
    remain_data_param = []
    for data_tmp,data_param_tmp in zip(data,data_param):
        if data_param_tmp['cc'] == 'dctcp':
            remain_data.append(data_tmp)
            remain_data_param.append(data_param_tmp)
        
        
    # Filter based on the existence of corresponding files
    # filtered_data = [item for item in data if not os.path.exists(f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{item['id']}/ns3/records.csv")]
    # filtered_data = [item for item_idx, item in enumerate(data) if not os.path.exists(f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{item_idx}/ns3-param/records.csv")]
    print(len(remain_data))
    # Export the filtered data to a new JSON file
    with open(f"{output_file}.mix.json", 'w') as f:
        json.dump(remain_data, f, indent=2)
    with open(f"{output_file}_param.mix.json", 'w') as f:
        json.dump(remain_data_param, f, indent=2)

# def sub_json(json_file, json_file_sub, output_file):
#     # Read the JSON file
#     with open(json_file, 'r') as f:
#         data = json.load(f)
    
#     with open(json_file_sub, 'r') as f:
#         data_sub = json.load(f)
#     filter_list = set([x['id'] for x in data_sub])
#     # Filter based on the existence of corresponding files
#     filtered_data = [item for item in data if not item['id'] in filter_list]
#     print(len(filtered_data))
#     # Export the filtered data to a new JSON file
#     with open(output_file, 'w') as f:
#         json.dump(filtered_data, f, indent=2)
    
# Example usage
json_file_path = 'all.mix.json'  # Replace with your JSON file path
json_file_path_param = 'all_param_window.mix.json'  # Replace with your JSON file path
output_file_path = 'remain'  # Replace with the desired output file path
filter_json(json_file_path, json_file_path_param,output_file_path)

