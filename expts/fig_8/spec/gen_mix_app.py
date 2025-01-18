import json
import random
import numpy as np


# Fix the random seed for reproducibility
def fix_seed(seed):
    np.random.seed(seed)
    random.seed(seed)


# Generate a list of configurations
def generate_config_list(output_file, num_configs, max_inflight_flows_list):
    # Define ranges and lists
    spatials = [
        "../../workload/spatials/cluster_d_4_16.json",
        "../../workload/spatials/cluster_d_4_16.json",
        "../../workload/spatials/cluster_d_4_16.json",
    ]
    clusters = [
        "spec/cluster_1_to_1_eval_test.json",
        "spec/cluster_2_to_1_eval_test.json",
        "spec/cluster_4_to_1_eval_test.json",
    ]

    size_dists = [
        "../../workload/distributions/facebook/webserver-all.txt",
        "../../workload/distributions/facebook/hadoop-all.txt",
        "../../workload/distributions/facebook/cachefollower-all.txt",
    ]

    lognorm_sigmas = [1.0, 2.0]
    max_loads = [0.30, 0.80]

    bfszs = [16, 16]
    windows = [5000, 15000]
    pfcs = [1.0, 1.0]
    # ccs = ["dctcp", "dcqcn", "hp", "timely"]
    ccs = ["dctcp", "dcqcn", "timely"]
    params = {
        "dctcp": {
            "k": [10, 30],
        },
        "dcqcn": {
            "k_min": [10.0, 30.0],
            "k_max": [30.0, 50.0],
        },
        "hp": {
            "ita": [70.0, 95.0],
            "hpai": [50.0, 100.0],
        },
        "timely": {
            "t_low": [40.0, 60.0],
            "t_high": [100.0, 150.0],
        },
    }

    # Create configurations
    config_list = []
    for config_id in range(num_configs):
        spatial = random.choice(spatials)
        size_dist = random.choice(size_dists)
        lognorm_sigma = random.choice(lognorm_sigmas)
        max_load = random.uniform(*max_loads)
        cluster = random.choice(clusters)
        bfsz = random.uniform(*bfszs)
        window = random.uniform(*windows)
        enable_pfc = random.choice(pfcs)
        cc = random.choice(ccs)
        if cc == "dctcp":
            param_1 = random.uniform(*params[cc]["k"])
            param_2 = 0
        elif cc == "dcqcn":
            param_1 = random.uniform(*params[cc]["k_min"])
            param_2 = random.uniform(*params[cc]["k_max"])
        elif cc == "hp":
            param_1 = random.uniform(*params[cc]["ita"])
            param_2 = random.uniform(*params[cc]["hpai"])
        elif cc == "timely":
            param_1 = random.uniform(*params[cc]["t_low"])
            param_2 = random.uniform(*params[cc]["t_high"])

        config = {
            "id": config_id,
            "spatial": spatial,
            "size_dist": size_dist,
            "lognorm_sigma": lognorm_sigma,
            "max_load": max_load,
            "cluster": cluster,
            "param_id": 0,
            "bfsz": bfsz,
            "window": int(window),
            "enable_pfc": int(enable_pfc),
            "cc": cc,
            "param_1": param_1,
            "param_2": param_2,
            "max_inflight_flows": 0,
        }

        config_list.append(config)
    config_list_repeat = []
    for max_inflight_flows in max_inflight_flows_list:
        for config in config_list:
            config_tmp = config.copy()
            config_tmp["id"] = len(config_list_repeat)
            config_tmp["max_inflight_flows"] = max_inflight_flows
            config_list_repeat.append(config_tmp)
    # Write the configurations to a JSON file
    with open(output_file, "w") as json_file:
        json.dump(config_list_repeat, json_file, indent=4)

    print(f"Generated {len(config_list_repeat)} configurations saved to {output_file}")


if __name__ == "__main__":
    # Set seed for reproducibility
    fix_seed(42)

    # Specify the output JSON file and parameters

    num_configs = 10
    output_file = "eval_test_app.mix.json"
    # max_inflight_flows_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    max_inflight_flows_list = [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29]
    # Generate configurations
    generate_config_list(
        output_file,
        num_configs,
        max_inflight_flows_list,
    )
