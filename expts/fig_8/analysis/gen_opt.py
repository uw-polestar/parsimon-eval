import numpy as np
import os
import pandas as pd
import sys

save_path="/data2/lichenni/ns3"
MTU=1000
BDP = 15 * MTU
bin_size_list=[MTU, BDP, 5 * BDP]

NR_PATHS_SAMPLED=1000
n_size_bucket_list_output=4
n_percentiles=20
N = 100
def main(sample_mode=0,n_mix=192):
    res=[]
    print(f"sample_mode: {sample_mode}, n_mix: {n_mix}")
    for mix_id in range(n_mix):
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
                    
        assert n_path_sampled==len(path_to_info)
        
        # n_sampled_paths=np.sum([path_to_info[key][0] for key in path_to_info])
        path_to_n_flows=np.array([len(path_to_flowid[key]) for key in path_to_flowid])
        # print(f"n_sampled_paths/total_path: {n_sampled_paths}/{len(path_to_n_flows)},{n_sampled_paths/len(path_to_n_flows)}")

        # n_flows_in_sampled_paths=np.sum([path_to_info[key][1] for key in path_to_info])
        # n_flows_in_paths=np.sum(path_to_n_flows)
        # print(f"n_sampled_flows/total_flows: {n_flows_in_sampled_paths}/{n_flows_in_paths},{n_flows_in_sampled_paths/n_flows_in_paths}")

        df_ns3 = pd.read_csv(f'{mix_dir}/ns3/records.csv')
        df_pmn_m = pd.read_csv(f'{mix_dir}/pmn-m/records.csv')
        flowIds=np.array(df_ns3['flow_id'])
        sizes_ns3=np.array(df_ns3['size'])
        sizes_pmn=np.array(df_pmn_m['size'])

        sldn_ns3=df_ns3['slowdown']
        sldn_pmn_m=df_pmn_m['slowdown']

        flowId_to_sldn_size={}  
        for i in range(df_ns3.shape[0]):
            flowId_to_sldn_size[flowIds[i]]=[sldn_ns3[i],sizes_ns3[i]]
        
        sldn_mlsys=[]
        sizes_mlsys=[]
        if sample_mode==0:
            path_sampled_list=np.random.choice(list(path_to_flowid.keys()), NR_PATHS_SAMPLED, replace=False) 
        elif sample_mode==1:
            prob=path_to_n_flows/np.sum(path_to_n_flows)
            path_sampled_list=np.random.choice(list(path_to_flowid.keys()), NR_PATHS_SAMPLED, p=prob, replace=True)
        elif sample_mode==2:
            flowid_to_path={}
            for path in path_to_flowid:
                for flowid in path_to_flowid[path]:
                    assert flowid not in flowid_to_path
                    flowid_to_path[flowid]=path
            # prob=path_to_n_flows/np.sum(path_to_n_flows)
            # sorted_indices = np.argsort(prob)[::-1]
            # top_indices = sorted_indices[:NR_PATHS_SAMPLED]
            # path_sampled_list= np.array(list(path_to_flowid.keys()))[top_indices]
            
            flow_sampled_list=np.random.choice(list(flowid_to_path.keys()), NR_PATHS_SAMPLED, replace=False) 
            path_sampled_list=[]
            flowid_blacklist=set([])
            for flowid in flow_sampled_list:
                if flowid not in flowid_blacklist:
                    path=flowid_to_path[flowid]
                    path_sampled_list.append(path)
                    flowid_blacklist.update(path_to_flowid[path])
            # path_sampled_list=list(set(path_sampled_list))
            path_sampled_list=np.random.choice(path_sampled_list, min(len(path_sampled_list),NR_PATHS_SAMPLED), replace=False)
            print(len(path_sampled_list))
        
        for key in path_sampled_list:
            flowid_list=path_to_flowid[key]
            if sample_mode==2:
                sldn_mlsys.extend([flowId_to_sldn_size[flowid][0] for flowid in flowid_list if flowid in flow_sampled_list])
                sizes_mlsys.extend([flowId_to_sldn_size[flowid][1] for flowid in flowid_list if flowid in flow_sampled_list])
            elif sample_mode==1:
                # Generate a random-sized array
                tmp_sldn=np.array([flowId_to_sldn_size[flowid][0] for flowid in flowid_list])
                tmp_size=np.array([flowId_to_sldn_size[flowid][1] for flowid in flowid_list])
                
                new_indices = np.linspace(0, len(tmp_sldn) - 1, N)
                tmp_sldn = np.interp(new_indices, np.arange(len(tmp_sldn)), tmp_sldn)
                tmp_size = np.interp(new_indices, np.arange(len(tmp_size)), tmp_size)
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

        bin_ns3=np.digitize(sizes_ns3, bin_size_list)
        bin_pmn=np.digitize(sizes_pmn, bin_size_list)
        bin_mlsys=np.digitize(sizes_mlsys, bin_size_list)
            
        for i in range(len(bin_size_list)+1):
            tmp_sldn_ns3 = np.extract(bin_ns3==i, sldn_ns3)
            tmp_sldn_pmn_m = np.extract(bin_pmn==i, sldn_pmn_m)
            tmp_sldn_mlsys=np.extract(bin_mlsys==i, sldn_mlsys)
            # tmp_sldn_mlsys = np.pad(tmp_sldn_mlsys, (0,len(tmp_sldn_ns3)-len(tmp_sldn_mlsys)), constant_values=1)
            # tmp_sldn_mlsys = np.pad(tmp_sldn_mlsys, (0,len(tmp_sldn_mlsys)), 
            sldn_ns3_p99=np.percentile(tmp_sldn_ns3,99)
            sldn_pmn_m_p99=np.percentile(tmp_sldn_pmn_m,99)
            df_mlsys_p99=np.percentile(tmp_sldn_mlsys,99)
            res_tmp.append([sldn_ns3_p99,sldn_pmn_m_p99,df_mlsys_p99])
        res.append(res_tmp)
    res = np.array(res)
    np.save(f'./gen_opt_{sample_mode}.npy',res)
    print(sample_mode,res.shape)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py arg1 arg2")
        sys.exit(1)
    main(sample_mode= int(sys.argv[1]),n_mix=int(sys.argv[2]))