import numpy as np
import os
import pandas as pd
import sys
from collections import defaultdict

save_path="/data2/lichenni/ns3"
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

def fix_seed(seed):
    # os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"
    np.random.seed(seed)

NR_PATHS_SAMPLED_LIST=[100,500,1000,10000]
# NR_PATHS_SAMPLED_LIST=[-1]
N_LIST=[100]
# NR_PATHS_SAMPLED_LIST=[10000]
# N_LIST=[50, 100, 500, 1000, 5000, 10000]
def main(sample_mode,n_mix,min_length,enable_percentile,enable_uniform):
    percentile_str="_percentile" if enable_percentile else ""
    uniform_str="_uniform" if enable_uniform else ""
    if not enable_uniform:
        PERCENTILE_LIST = np.array(
            [1, 10, 25, 40, 55, 70, 75, 80, 85, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100]
        )
    else:
        PERCENTILE_LIST = np.arange(0.0, 101.0, 5.0)
    
    for NR_PATHS_SAMPLED in NR_PATHS_SAMPLED_LIST:
        for N in N_LIST:
            res=[]
            print(f"sample_mode: {sample_mode}, n_mix: {n_mix}, min_length: {min_length}, NR_PATHS_SAMPLED: {NR_PATHS_SAMPLED}, N: {N}, enable_percentile: {enable_percentile}, enable_uniform: {enable_uniform}")
            for mix_id in range(n_mix):
            # for mix_id in [4,7,0]:
                print(f"mix_id: {mix_id}")
                res_tmp=[]
                mix_dir = f'../data/{mix_id}'
                # Accuracy metrics
                file_path_to_flowId=f'{save_path}/path_to_flowId_{mix_id}.npz'
                if not os.path.exists(file_path_to_flowId):
                    path_to_info={}
                    path_to_flowid={}
                    n_path_sampled=0
                    with open(f'{mix_dir}/mlsys/path.txt', 'r') as file:
                        for line_idx,line in enumerate(file):
                            if line_idx==0:
                                n_path_sampled=int(line.strip().split(",")[1])
                            elif line_idx==1:
                                path_list = line.strip()[:-1].split(",")
                                for path in path_list:
                                    parts=path.split(":")
                                    # the sampling frequency of the path and the number of flows in the path
                                    path_to_info[parts[0]]=[int(parts[1]),int(parts[2])]
                            else:
                                # Remove leading and trailing whitespaces
                                parts = line.strip().split(':')
                                path, numbers_part = parts[0], parts[1]
                                flowid_list = [int(x) for x in numbers_part.split(',')]
                                
                                path_to_flowid[path]=flowid_list
                    np.savez(file_path_to_flowId, n_path_sampled=n_path_sampled,path_to_info=path_to_info,path_to_flowid=path_to_flowid)
                else:
                    data=np.load(file_path_to_flowId, allow_pickle=True)
                    n_path_sampled=data['n_path_sampled']
                    path_to_info=data['path_to_info'].item()
                    path_to_flowid=data['path_to_flowid'].item()
                            
                # assert n_path_sampled==len(path_to_info)
                path_to_n_flows=np.array([len(path_to_flowid[key]) for key in path_to_flowid])
                
                path_to_flowid = {key: value for key, value in path_to_flowid.items() if len(value) >= min_length}
                
                # n_sampled_paths=np.sum([path_to_info[key][0] for key in path_to_info])
                
                # flow_ratio=np.sum(path_to_n_flows)/n_flows_total
                # padding_ratio=(1-flow_ratio)/flow_ratio
                # print(f"ratio: {flow_ratio},{padding_ratio}")
                # print(f"n_sampled_paths/total_path: {n_sampled_paths}/{len(path_to_n_flows)},{n_sampled_paths/len(path_to_n_flows)}")

                # n_flows_in_sampled_paths=np.sum([path_to_info[key][1] for key in path_to_info])
                # n_flows_in_paths=np.sum(path_to_n_flows)
                # print(f"n_sampled_flows/total_flows: {n_flows_in_sampled_paths}/{n_flows_in_paths},{n_flows_in_sampled_paths/n_flows_in_paths}")

                df_ns3 = pd.read_csv(f'{mix_dir}/ns3/records.csv')
                df_pmn_m = pd.read_csv(f'{mix_dir}/pmn-m/records.csv')
                sizes_ns3=np.array(df_ns3['size'])
                # sizes_pmn=np.array(df_pmn_m['size'])
                flowIds=np.array(df_ns3['flow_id'])
                
                sldn_ns3=df_ns3['slowdown']
                sldn_pmn_m=df_pmn_m['slowdown']

                flowId_to_sldn_size={}  
                for i in range(df_ns3.shape[0]):
                    flowId_to_sldn_size[flowIds[i]]=[sldn_ns3[i],sizes_ns3[i]]
                    
                sldn_mlsys=[]
                sizes_mlsys=[]
                if sample_mode==0:
                    if NR_PATHS_SAMPLED==-1:
                        path_sampled_list=list(path_to_flowid.keys())
                    else:
                        path_sampled_list=np.random.choice(list(path_to_flowid.keys()), NR_PATHS_SAMPLED, replace=False) 
                elif sample_mode==1:
                    prob=path_to_n_flows/np.sum(path_to_n_flows)
                    path_sampled_list=np.random.choice(list(path_to_flowid.keys()), NR_PATHS_SAMPLED, p=prob, replace=True)
                    path_count=defaultdict(lambda:0)
                    for path in path_sampled_list:
                        path_count[path]+=1
                    path_sampled_list=list(set(path_sampled_list))
                elif sample_mode==2:
                    flowid_to_path={}
                    for path in path_to_flowid:
                        for flowid in path_to_flowid[path]:
                            assert flowid not in flowid_to_path
                            flowid_to_path[flowid]=path
                
                    flow_sampled_list=np.random.choice(list(flowid_to_path.keys()), NR_PATHS_SAMPLED, replace=False) 
                    path_count=defaultdict(lambda:0)
                    for flowid in flow_sampled_list:
                        path=flowid_to_path[flowid]
                        path_count[path]+=1
                    path_sampled_list=list(path_count.keys())
                print(f"{len(path_sampled_list)}/{NR_PATHS_SAMPLED}")
                for _,path in enumerate(path_sampled_list):
                    flowid_list=path_to_flowid[path]
                    if sample_mode>0:
                        if enable_percentile:
                            tmp=np.array([flowId_to_sldn_size[flowid] for flowid in flowid_list])
                            sorted_indices = np.lexsort((tmp[:, 1], tmp[:, 0]))
                            tmp=tmp[sorted_indices]
                            index_list=np.percentile(np.arange(tmp.shape[0]), PERCENTILE_LIST).astype(int)
                            
                            sldn_percentile = np.percentile(tmp[:, 0], PERCENTILE_LIST)
                            size_percentile = np.array([tmp[i, 1] for i in index_list])
                            
                            target_percentiles=np.random.uniform(0, 100.0, size=N)
                            tmp_sldn=recover_data(PERCENTILE_LIST, sldn_percentile,target_percentiles)
                            tmp_size=recover_data(PERCENTILE_LIST, size_percentile,target_percentiles)
                            
                            for _ in range(path_count[path]):
                                sldn_mlsys.extend(tmp_sldn)
                                sizes_mlsys.extend(tmp_size)
                        else:
                            if sample_mode==2:
                                sldn_mlsys.extend([flowId_to_sldn_size[flowid][0] for flowid in flowid_list if flowid in flow_sampled_list])
                                sizes_mlsys.extend([flowId_to_sldn_size[flowid][1] for flowid in flowid_list if flowid in flow_sampled_list])
                            else:
                                tmp=np.array([flowId_to_sldn_size[flowid] for flowid in flowid_list])
                                # sorted_indices = np.lexsort((tmp[:, 1], tmp[:, 0]))
                                # tmp=tmp[sorted_indices]

                                # sldn_percentile = tmp[:, 0]
                                # size_percentile = tmp[:, 1]
                        
                                # n_points=len(sldn_percentile)
                                # n_points_list=np.arange(n_points)
                                
                                # new_indices = np.linspace(0, n_points - 1, N)
                                # tmp_sldn = np.interp(new_indices, n_points_list, sldn_percentile)
                                # tmp_size = np.interp(new_indices, n_points_list, size_percentile)
                                
                                tmp_sampled=np.random.choice(np.arange(tmp.shape[0]), N, replace=True)
                                tmp_sldn=tmp[tmp_sampled,0]
                                tmp_size=tmp[tmp_sampled,1]            
                                for _ in range(path_count[path]):
                                    sldn_mlsys.extend(tmp_sldn)
                                    sizes_mlsys.extend(tmp_size)
                    else:
                        # for _ in range(path_to_info[key][0]):
                        sldn_mlsys.extend([flowId_to_sldn_size[flowid][0] for flowid in flowid_list])
                        sizes_mlsys.extend([flowId_to_sldn_size[flowid][1] for flowid in flowid_list])
                sldn_mlsys=np.array(sldn_mlsys)
                
                sldn_mlsys_len=len(sldn_mlsys)

                sldn_ns3_p99=np.percentile(sldn_ns3,99)
                sldn_pmn_m_p99=np.percentile(sldn_pmn_m,99)
                sldn_mlsys_p99=np.percentile(sldn_mlsys,99)

                print("sldn_ns3: ",sldn_ns3_p99," sldn_pmn_m: ", sldn_pmn_m_p99," sldn_mlsys: ", sldn_mlsys_p99)

                res_tmp.append([sldn_ns3_p99,sldn_pmn_m_p99,sldn_mlsys_p99])
                
                print(f"df_ns3: {df_ns3.shape[0]}, df_pmn_m: {df_pmn_m.shape[0]}, df_mlsys: {sldn_mlsys_len}")

                # bin_ns3=np.digitize(sizes_ns3, bin_size_list)
                # bin_pmn=np.digitize(sizes_pmn, bin_size_list)
                # bin_mlsys=np.digitize(sizes_mlsys, bin_size_list)
                    
                # for i in range(len(bin_size_list)+1):
                #     tmp_sldn_ns3 = np.extract(bin_ns3==i, sldn_ns3)
                #     tmp_sldn_pmn_m = np.extract(bin_pmn==i, sldn_pmn_m)
                #     tmp_sldn_mlsys=np.extract(bin_mlsys==i, sldn_mlsys)
                
                #     sldn_ns3_p99=np.percentile(tmp_sldn_ns3,99)
                #     sldn_pmn_m_p99=np.percentile(tmp_sldn_pmn_m,99)
                #     df_mlsys_p99=np.percentile(tmp_sldn_mlsys,99)
                #     res_tmp.append([sldn_ns3_p99,sldn_pmn_m_p99,df_mlsys_p99])
                res.append(res_tmp)
            res = np.array(res)
            print(sample_mode,res.shape)
            np.save(f'./gen_opt_{sample_mode}_{min_length}_{NR_PATHS_SAMPLED}_{N}{percentile_str}{uniform_str}_n.npy',res)

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python script.py arg1 arg2 arg3")
        sys.exit(1)
    fix_seed(0)
    main(sample_mode= int(sys.argv[1]),n_mix=int(sys.argv[2]),min_length=int(sys.argv[3]),enable_percentile=bool(int(sys.argv[4])),enable_uniform=bool(int(sys.argv[5])))