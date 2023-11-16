#! /usr/bin/env python3

import numpy as np
import click
import json
import random
from scipy.ndimage import zoom


@click.command()
@click.option('--input_file', type=click.Path(exists=True), help="Input JSON file with the original spatial")
@click.option('--output_file', type=click.Path(), help="Output JSON file with the upscaled spatial")
@click.option('--zoom_factor', type=int, help="Zoom factor for upscaling the matrix")
def upscale(input_file, output_file, zoom_factor):
    # Load the original matrix
    with open(input_file, 'r') as f:
        spatial = json.load(f)
        inner = np.array(spatial['matrix']['inner'])

    # Upscale the matrix
    upscaled_inner = zoom(inner, zoom_factor)
    upscaled_inner = np.maximum(0, upscaled_inner)
    upscaled_nr_racks = len(upscaled_inner)
    upscaled_racks = [f'rack_{i}' for i in range(upscaled_nr_racks)]
    upscaled_matrix = {'inner': upscaled_inner.tolist(),
                       'idx2name': upscaled_racks}

    upscaled_nr_pods = spatial['nr_pods'] * zoom_factor
    upscaled_pods = [f"pod_{i}" for i in range(upscaled_nr_pods)]
    random.shuffle(upscaled_racks)
    upscaled_pod2tors = {pod: [] for pod in upscaled_pods}
    for i, rack in enumerate(upscaled_racks):
        upscaled_pod2tors[upscaled_pods[i // 48]].append(rack)
    upscaled_spatial = {'matrix': upscaled_matrix, 'pod2tors': upscaled_pod2tors,
                        'nr_pods': upscaled_nr_pods, 'nr_racks': upscaled_nr_racks}

    with open(output_file, 'w') as f:
        json.dump(upscaled_spatial, f, indent=4)

    print("Upscaling completed successfully!")


if __name__ == "__main__":
    upscale()
