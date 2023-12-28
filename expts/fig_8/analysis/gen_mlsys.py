import pandas as pd
import json
import os
import altair as alt
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import wasserstein_distance
# sns.set(style='ticks', context='paper', font='CMU Sans Serif')
sns.set(style='ticks', context='paper')
from collections import defaultdict
import numpy as np
import subprocess
from util import plot_cdf

def pn_distance(a, b, p):
    x = a.quantile(p)
    y = b.quantile(p)
    return (y - x) / x
    
def p99_distance(a, b):
    return pn_distance(a, b, 0.99)
NR_PATHS_SAMPLED=1000
n_size_bucket_list_output=4
n_percentiles=20
# P99_PERCENTILE_LIST = np.array(
#     [10, 20, 30, 40, 50, 60, 70, 75, 80, 85, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]
# )
P99_PERCENTILE_LIST = np.array(
    [10, 25, 40, 55, 70, 75, 80, 85, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 99.9, 99.99]
)
MTU=1000
BDP = 15 * MTU
bin_size_list=[MTU, BDP, 5 * BDP]

def recover_data(sampling_percentiles, sampled_data,target_percentiles):
    recovered_data = []

    for percentile in target_percentiles:
        # Find the two nearest percentiles in the sampled data
        lower_percentile = max(filter(lambda x: x <= percentile, sampling_percentiles), default=0)
        upper_percentile = min(filter(lambda x: x >= percentile, sampling_percentiles), default=100)

        # Retrieve corresponding values from the sampled data
        lower_index = np.where(sampling_percentiles == lower_percentile)[0][0] if lower_percentile in sampling_percentiles else 0
        upper_index = np.where(sampling_percentiles == upper_percentile)[0][0] if upper_percentile in sampling_percentiles else -1

        lower_value = sampled_data[lower_index]
        upper_value = sampled_data[upper_index]

        # Interpolate to recover the original data
        recovered_value = np.interp(percentile, [lower_percentile, upper_percentile], [lower_value, upper_value])

        # Append the recovered value to the list
        recovered_data.append(recovered_value)

    return recovered_data

target_percentiles = np.arange(1, 101, 1)

mlsys_dir="mlsys_s2_bt50_2k"
save_file=f'./gen_{mlsys_dir}_dedupe.npy'
# save_file=f'./gen_{mlsys_dir}.npy'
if not os.path.exists(save_file):
    res_final=[]
    for worst_low_id in range(0,192):
    # for worst_low_id in range(0,40):
        res_tmp=[]
        mix_dir = f'../data/{worst_low_id}'
        # Accuracy metrics
        df_ns3 = pd.read_csv(f'{mix_dir}/ns3/records.csv')
        df_pmn_m = pd.read_csv(f'{mix_dir}/pmn-m/records.csv')
        df_mlsys = []
        n_flows_list=[]
        path_idx=0
        mix_dir=f'/data2/lichenni/data_10m/{worst_low_id}'
        while os.path.exists(f'{mix_dir}/{mlsys_dir}/{path_idx}/fct_mlsys.txt'):
            with open(f'{mix_dir}/{mlsys_dir}/path_{path_idx}.txt', 'r') as file:
                # n_freq=int(file.readline().strip().split(",")[-1])
                # number of flows in the path
                n_freq=1
                n_flows=int(file.readline().strip().split(",")[-3])
                n_flows_list.append(n_flows)
            with open(f'{mix_dir}/{mlsys_dir}/{path_idx}/fct_mlsys.txt', 'r') as file:
                data = np.array([float(value) for line in file for value in line.split()])
                assert data.shape[0] == n_size_bucket_list_output*n_percentiles
                for _ in range(n_freq):
                    tmp_list=[]
                    tmp=data.reshape(n_size_bucket_list_output, n_percentiles)
                    for i in range(n_size_bucket_list_output):
                        tmp_list.append(recover_data(P99_PERCENTILE_LIST, tmp[i].tolist(),target_percentiles))
                    df_mlsys.append(np.array(tmp_list))
            path_idx+=1
            
        df_mlsys_tmp=np.array(df_mlsys)
        
        weight=np.array(n_flows_list)/np.sum(n_flows_list)
        df_mlsys=np.average(df_mlsys_tmp,axis=0,weights=weight)
        
        # df_mlsys=[]
        # for i in range(n_size_bucket_list_output):
        #     df_mlsys.append(df_mlsys_tmp[:,i,:].flatten())
        # df_mlsys=np.array(df_mlsys)

        sizes_ns3=np.array(df_ns3['size'])
        sizes_pmn=np.array(df_pmn_m['size'])
        bin_ns3=np.digitize(sizes_ns3, bin_size_list)
        bin_pmn=np.digitize(sizes_pmn, bin_size_list)
        
        # Count occurrences of each bin index
        bin_counts = np.bincount(bin_ns3)
        # Calculate the total count
        total_count = np.sum(bin_counts)
        # Calculate the ratio for each bucket
        bucket_ratios = bin_counts / total_count
        sldn_mlsys=np.sum(np.multiply(df_mlsys.T, bucket_ratios).T,axis=0)

        sldn_ns3=df_ns3['slowdown']
        sldn_pmn_m=df_pmn_m['slowdown']
        # print(f"{worst_low_id}: path={src_dst_pair}, len=",df_ns3.shape[0],df_pmn_m.shape[0],df_ns3_path.shape[0],df_flowsim.shape[0])
        sldn_ns3_p99=np.percentile(sldn_ns3,99)
        sldn_pmn_m_p99=np.percentile(sldn_pmn_m,99)
        sldn_mlsys_p99=np.percentile(sldn_mlsys,99)
            
        print("sldn_ns3: ",sldn_ns3_p99," sldn_pmn_m: ", sldn_pmn_m_p99," sldn_mlsys: ", sldn_mlsys_p99)

        res_tmp.append([sldn_ns3_p99,sldn_pmn_m_p99,sldn_mlsys_p99])

        # assert df_ns3.shape[0]==df_pmn_m.shape[0]==df_ns3_path.shape[0]==sldn_flowsim.shape[0]
        print(f"df_ns3: {df_ns3.shape[0]}, df_pmn_m: {df_pmn_m.shape[0]}, df_mlsys: {df_mlsys.shape[1]}")
        
        bin_ns3=np.digitize(sizes_ns3, bin_size_list)
        bin_pmn=np.digitize(sizes_pmn, bin_size_list)
        for i in range(len(bin_size_list)+1):
            tmp_sldn_ns3 = np.extract(bin_ns3==i, sldn_ns3)
            tmp_sldn_pmn_m = np.extract(bin_pmn==i, sldn_pmn_m)
            tmp_sldn_mlsys=df_mlsys[i]
            
            sldn_ns3_p99=np.percentile(tmp_sldn_ns3,99)
            sldn_pmn_m_p99=np.percentile(tmp_sldn_pmn_m,99)
            df_mlsys_p99=np.percentile(tmp_sldn_mlsys,99)
            res_tmp.append([sldn_ns3_p99,sldn_pmn_m_p99,df_mlsys_p99])
        res_final.append(res_tmp)
    res_final = np.array(res_final)
    np.save(save_file,res_final)
else:
    res_final=np.load(save_file)


