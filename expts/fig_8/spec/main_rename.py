import os

base_directory = "/data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/"
old_suffix = "_k30.txt"
new_suffix = "_k18000.txt"

try:
    cnt=0
    for mix_id in range(192):
        if not os.path.exists(f"{base_directory}/{mix_id}/ns3-param/records.csv"): continue
        print(f"mix_id: {mix_id}")
        subdirectory = f"{mix_id}/ns3-param/"
        directory_to_rename = os.path.join(base_directory, subdirectory)

        for root, _, files in os.walk(directory_to_rename):
            for filename in files:
                if filename.endswith(old_suffix):
                    old_path = os.path.join(root, filename)
                    new_filename = filename.replace(old_suffix, new_suffix)
                    new_path = os.path.join(root, new_filename)

                    # os.rename(old_path, new_path)
                    # print(f"Renamed: {old_path} to {new_path}")
        cnt+=1

    print(f"{cnt} eligible dirs renamed successfully.")
except FileNotFoundError:
    print(f"Directory not found: {directory_to_rename}")
except Exception as e:
    print(f"An error occurred: {str(e)}")
