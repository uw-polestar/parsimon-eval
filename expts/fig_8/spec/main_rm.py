import shutil
import json
json_file='./test_mlsys.mix.json'
with open(json_file, 'r') as f:
    data = json.load(f)

try:
    cnt=0
    for mix in data:
        mix_id=mix['id']
        directory_to_remove = f"/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/{mix_id}/ns3-param"

        # shutil.rmtree(directory_to_remove)
        print(f"{cnt}th directory '{directory_to_remove}' removed successfully.")
        cnt+=1
except FileNotFoundError:
    print(f"Directory not found: {directory_to_remove}")
except OSError as e:
    print(f"An error occurred: {str(e)}")
