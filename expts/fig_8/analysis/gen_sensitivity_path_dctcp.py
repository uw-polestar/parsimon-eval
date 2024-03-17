import pandas as pd
import os
import numpy as np

MTU=1000
BDP = 10 * MTU
bin_size_list=[MTU, BDP, 5 * BDP]

n_size_bucket_list_output=len(bin_size_list)+1

N_FLOW_THRESHOLD_LIST=[1,0,5,10]
NR_PATHS_SAMPLED=500
NR_INTEPOLATE=100
N_FLOWS=NR_PATHS_SAMPLED*NR_INTEPOLATE*4
N_FLOWS_PER_PATH=NR_INTEPOLATE*4
enable_sample_per_path=False
sample_per_path_str="_samp" if enable_sample_per_path else "_nosamp"
shard_seed=2
mlsys_dir_list=[f"mlsys-new_e533_s{shard_seed}"]
legend_list=['ns3-config',f'pmn-m_s{shard_seed}',"mlsys"]
for N_FLOW_THRESHOLD in N_FLOW_THRESHOLD_LIST:
    print("N_FLOW_THRESHOLD: ",N_FLOW_THRESHOLD)
    res=[]
    for mlsys_dir_idx,mlsys_dir in enumerate(mlsys_dir_list):
        save_file=f'./config_gen_{mlsys_dir}_p{NR_PATHS_SAMPLED}_l{NR_INTEPOLATE}_t{N_FLOW_THRESHOLD}{sample_per_path_str}.npz'
        if not os.path.exists(save_file):
            res_final=[]
            n_flows_in_f_list_final=[]
            for worst_low_id in range(192):
                mix_dir = f'../data/{worst_low_id}'
                if not os.path.exists(f'{mix_dir}/{legend_list[0]}/elapsed_{shard_seed}.txt'):
                    print(f'{worst_low_id}: No GT!')
                    res_final.append(np.zeros((n_size_bucket_list_output+1,3)))
                    n_flows_in_f_list_final.append(np.zeros(NR_PATHS_SAMPLED))
                    continue
                
                res_tmp=[]
                df_pmn_m = pd.read_csv(f'{mix_dir}/{legend_list[1]}/records.csv')
                
                n_freq_list=[]
                n_flows_in_f_list=[]
                n_flow_list=[]
                sizes_pmn=df_pmn_m['size']
                
                path_idx=0
                while os.path.exists(f'{mix_dir}/{mlsys_dir}/path_{path_idx}.txt'):
                    with open(f'{mix_dir}/{mlsys_dir}/path_{path_idx}.txt', 'r') as file:
                        lines = file.readlines()
                        data=lines[0].strip().split(",")
                        n_freq=int(data[-1])
                        n_freq_list.append(n_freq)
                        for _ in range(n_freq):
                            n_flows_in_f_list.append(int(data[-3]))
                        
                        flowid_list=[int(tmp) for tmp in lines[2].strip().split(",")]
                        size_list=[sizes_pmn[flowid] for flowid in flowid_list]
                        
                        n_links=len(data[0].split("|"))-1
                        tmp=np.digitize(size_list, bin_size_list)
                        # Count occurrences of each bin index
                        bin_counts = np.zeros(n_size_bucket_list_output)
                        for bin_idx in tmp:
                            bin_counts[bin_idx]+=1
                        n_flow_list.append(bin_counts)
                    path_idx+=1
                assert sum(n_freq_list)==NR_PATHS_SAMPLED
                n_flow_list=np.array(n_flow_list)
                n_flow_list_sum=n_flow_list.sum(axis=0)
                
                n_flows_in_f_list_final.append(n_flows_in_f_list)
                
                df_ns3 = pd.read_csv(f'{mix_dir}/{legend_list[0]}/records_{shard_seed}.csv')
                
                df_mlsys = [[] for _ in range(n_size_bucket_list_output)]
                
                sizes_ns3=np.array(df_ns3['size'])
                bin_ns3=np.digitize(sizes_ns3, bin_size_list)
                bin_pmn=np.digitize(sizes_pmn, bin_size_list)
                bin_counts = np.bincount(bin_ns3)
                total_count = np.sum(bin_counts)
                bucket_ratios = bin_counts / total_count
                
                with open(f'{mix_dir}/{mlsys_dir}/path.txt', 'r') as file:
                    lines = file.readlines()
                lines = lines[1:]
                for line_idx,line in enumerate(lines):
                    data=line.strip().split(",")
                    data = [float(value) for value in data]
                    assert len(data) == NR_INTEPOLATE
                    n_freq=n_freq_list[line_idx//n_size_bucket_list_output]
                    
                    if enable_sample_per_path:
                        n_flow_tmp=n_flow_list[line_idx//n_size_bucket_list_output]
                        n_flow_tmp=np.where(n_flow_tmp >= N_FLOW_THRESHOLD, n_flow_tmp, 0)
                        
                        if np.sum(n_flow_tmp)==0:
                            continue
                        prop_tmp=n_flow_tmp/np.sum(n_flow_tmp)
                        
                        num_tmp=int(N_FLOWS_PER_PATH*prop_tmp[line_idx%n_size_bucket_list_output])
                        
                        data_sampled=np.random.choice(data,num_tmp,replace=True)
                        for _ in range(n_freq):
                            df_mlsys[line_idx%n_size_bucket_list_output].extend(data_sampled)
                    else:
                        if n_flow_list[line_idx//n_size_bucket_list_output][line_idx%n_size_bucket_list_output]>=N_FLOW_THRESHOLD:
                            for _ in range(n_freq):
                                df_mlsys[line_idx%n_size_bucket_list_output].extend(data)
                    
                df_mlsys_shape=[len(df_mlsys[i]) for i in range(len(df_mlsys))]
                print(f"{worst_low_id}: {df_mlsys_shape}, {np.max(n_freq_list)}")
                print("bucket_ratios: ",bucket_ratios)
                bucket_ratios_sampled=n_flow_list_sum/sum(n_flow_list_sum)
                print("bucket_ratios_sampled: ",bucket_ratios_sampled)
                
                sldn_mlsys_p99=np.array([np.percentile(df_mlsys[i],99) for i in range(len(df_mlsys))])
                
                print("df_mlsys_p99: ",sldn_mlsys_p99)
                df_mlsys_total=[]
                for i in range(len(df_mlsys)):
                    # for _ in range(int(bucket_ratios[i]*100)):
                    if enable_sample_per_path:
                        df_mlsys_total.extend(df_mlsys[i])
                    else: 
                        n_tmp=int(N_FLOWS*bucket_ratios_sampled[i])
                        df_mlsys_total.extend(np.random.choice(df_mlsys[i],n_tmp,replace=True))
                sldn_mlsys_p99=np.percentile(df_mlsys_total,99)

                sldn_ns3=df_ns3['slowdown']
                sldn_pmn_m=df_pmn_m['slowdown']
                sldn_ns3_p99=np.percentile(sldn_ns3,99)
                sldn_pmn_m_p99=np.percentile(sldn_pmn_m,99)
                
                print("sldn_ns3: ",sldn_ns3_p99," sldn_pmn_m: ", sldn_pmn_m_p99," sldn_mlsys: ", sldn_mlsys_p99)

                res_tmp.append([sldn_ns3_p99,sldn_pmn_m_p99,sldn_mlsys_p99])

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
            n_flows_in_f_list_final = np.array(n_flows_in_f_list_final)
            print("res:", res_final.shape,n_flows_in_f_list_final.shape)
            np.savez(save_file,res_final=res_final,n_flows_in_f_list_final=n_flows_in_f_list_final)
        else:
            data=np.load(save_file)
            res_final=data['res_final']
            n_flows_in_f_list_final=data['n_flows_in_f_list_final']
        if mlsys_dir_idx==0:
            res.append(res_final[:,0,0].transpose())
            res.append(res_final[:,0,1].transpose())
            res.append(res_final[:,0,2].transpose())
        else:
            res.append(res_final[:,0,-1].transpose())

    res=np.array(res)
    print(res.shape)
    n_flows_median_list=np.median(n_flows_in_f_list_final,axis=1)
    # n_flows_median_list=n_flows_in_f_list_final
    print("n_flows_median_list: ",n_flows_median_list.shape)


