import json
import random

random.seed(0)
def generate_cases(sample_space, num_cases=200):
    cases = []

    while len(cases) < num_cases:
        case = {
            "id": len(cases), 
            "spatial": random.choice(sample_space["spatials"]),
            "size_dist": random.choice(sample_space["size_dists"]),
            "lognorm_sigma": random.choice(sample_space["lognorm_sigmas"]),
            "max_load": random.uniform(sample_space["max_loads"]["low"], sample_space["max_loads"]["high"]),
            "cluster": random.choice(sample_space["clusters"]),
            "cc": random.choice(sample_space["ccs"]),
            "param_cc": random.uniform(sample_space["params_cc"]["low"], sample_space["params_cc"]["high"]),
            "window": random.choice(sample_space["windows"])
        }

        if case not in cases:  # Ensure uniqueness
            cases.append(case)

    return cases
file_sample_space = './all_config.mixspace.json'  # Replace with your sample space file path
file_output= './all_config.mix.json'  # Replace with the desired output file path
with open(file_sample_space, 'r') as f:
        sample_space = json.load(f)
# Generate 200 unique cases
generated_cases = generate_cases(sample_space, 200)

# Display the first 5 cases to check variety
print(f"save {len(generated_cases)} cases")
with open(file_output, 'w') as f:
    json.dump(generated_cases, f, indent=2)