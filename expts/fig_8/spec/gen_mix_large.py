import json
import random
import numpy as np


# Fix the random seed for reproducibility
def fix_seed(seed):
    np.random.seed(seed)
    random.seed(seed)


# Generate a list of configurations
def generate_config_list(output_file, enable_empirical, num_configs):
    # Define ranges and lists
    if enable_empirical:
        spatials = [
            ("../../workload/spatials/cluster_a_4_16.json", "_4_16.json"),
            ("../../workload/spatials/cluster_b_8_16.json", "_8_16.json"),
            ("../../workload/spatials/cluster_c_8_16.json", "_8_16.json"),
        ]
        clusters = [
            "spec/cluster_1_to_1_eval",
            "spec/cluster_2_to_1_eval",
            "spec/cluster_4_to_1_eval",
        ]
    else:
        spatials = [
            "../../workload/spatials/cluster_a_2_4.json",
            "../../workload/spatials/cluster_b_2_4.json",
            "../../workload/spatials/cluster_c_2_4.json",
        ]
        clusters = [
            "spec/cluster_1_to_1_m4.json",
            "spec/cluster_2_to_1_m4.json",
            "spec/cluster_4_to_1_m4.json",
        ]

    if enable_empirical:
        size_dists = [
            "../../workload/distributions/facebook/webserver-all.txt",
            "../../workload/distributions/facebook/hadoop-all.txt",
            "../../workload/distributions/facebook/cachefollower-all.txt",
        ]
    else:
        size_dists = [
            f"../../workload/distributions/synthetic/sync-all-{i}.txt"
            for i in range(2000)
        ]

    lognorm_sigmas = [1.0, 2.0]
    max_loads = [0.30, 0.80]

    bfszs = [10, 18]
    windows = [10000, 18000]
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
            "spatial": spatial[0],
            "size_dist": size_dist,
            "lognorm_sigma": lognorm_sigma,
            "max_load": max_load,
            "cluster": cluster + spatial[1],
            "param_id": 0,
            "bfsz": bfsz,
            "window": int(window),
            "enable_pfc": int(enable_pfc),
            "cc": cc,
            "param_1": param_1,
            "param_2": param_2,
        }

        config_list.append(config)

    # Write the configurations to a JSON file
    with open(output_file, "w") as json_file:
        json.dump(config_list, json_file, indent=4)

    print(f"Generated {num_configs} configurations saved to {output_file}")


if __name__ == "__main__":
    # Set seed for reproducibility
    fix_seed(42)

    # Specify the output JSON file and parameters

    enable_empirical = True  # Change to False if you want synthetic workloads
    num_configs = 4000 if not enable_empirical else 100
    output_file = (
        "eval_train_new.mix.json"
        if not enable_empirical
        else "eval_test_new_large.mix.json"
    )

    # Generate configurations
    generate_config_list(output_file, enable_empirical, num_configs)
