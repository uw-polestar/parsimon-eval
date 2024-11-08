import json


def generate_config(output_file):
    # Define the paths and values
    spatials = [
        "../../workload/spatials/cluster_a_2_4.json",
        "../../workload/spatials/cluster_b_2_4.json",
        "../../workload/spatials/cluster_c_2_4.json",
    ]

    size_dists = [
        "../../workload/distributions/facebook/webserver-all.txt",
        "../../workload/distributions/facebook/hadoop-all.txt",
        "../../workload/distributions/facebook/cachefollower-all.txt",
    ]
    # size_dists = [
    #     f"../../workload/distributions/synthetic/sync-all-{i}.txt" for i in range(1000)
    # ]

    lognorm_sigmas = [1.0, 2.0]

    max_loads = {"low": 0.30, "high": 0.80}

    clusters = [
        "spec/cluster_1_to_1_m4.json",
        "spec/cluster_2_to_1_m4.json",
        "spec/cluster_4_to_1_m4.json",
    ]

    bfszs = {"low": 300, "high": 300}

    windows = {"low": 18, "high": 18}

    pfcs = [1.0, 1.0]

    ccs = ["dctcp"]

    params = [{"low": 30.0, "high": 30.0}, {"low": 0.0, "high": 0.0}]

    # Create the configuration dictionary
    config = {
        "spatials": spatials,
        "size_dists": size_dists,
        "lognorm_sigmas": lognorm_sigmas,
        "max_loads": max_loads,
        "clusters": clusters,
        "bfszs": bfszs,
        "windows": windows,
        "pfcs": pfcs,
        "ccs": ccs,
        "params": params,
    }

    # Write the configuration to a JSON file
    with open(output_file, "w") as json_file:
        json.dump(config, json_file, indent=4)

    print(f"Configuration file created at {output_file}")


if __name__ == "__main__":
    # Specify the output JSON file
    # output_file = "dctcp_sync.mixspace.json"
    output_file = "dctcp_empirical.mixspace.json"

    # Generate the config file
    generate_config(output_file)
