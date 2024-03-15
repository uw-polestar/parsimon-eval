import json
import os

def filter_json(json_file, output_file):
    # Read the JSON file
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    remain_data = []
    for data_tmp in data:
        if data_tmp['cc'] == 'timely':
            remain_data.append(data_tmp)
        
    print(len(remain_data))
    # Export the filtered data to a new JSON file
    with open(f"{output_file}.mix.json", 'w') as f:
        json.dump(remain_data, f, indent=2)
    
# Example usage
json_file_path = 'all_config_1.mix.json'  # Replace with your JSON file path
filter_json(json_file_path,"all_config_1_timely")

