# weighted_genpar_flowsim.py
import numpy as np
from ctypes import *

from time import time
import argparse
import os


class FCTStruct(Structure):
    _fields_ = [
        ("estimated_fcts", POINTER(c_double)),
        ("t_flows", POINTER(c_double)),
        ("num_flows", POINTER(c_uint)),
        ("num_flows_enq", POINTER(c_uint)),
    ]


def make_array(ctype, arr):
    return (ctype * len(arr))(*arr)


C_LIB_PATH = "/data1/lichenni/projects/flow_simulation/fast-mmf-fattree/get_fct_mmf.so"

C_LIB = CDLL(C_LIB_PATH)
C_LIB.get_fct_mmf = C_LIB.get_fct_mmf
C_LIB.get_fct_mmf.argtypes = [
    c_uint,
    POINTER(c_double),
    POINTER(c_double),
    POINTER(c_int),
    POINTER(c_int),
    c_int,
    POINTER(c_int),
    c_int,
    c_int,
    c_int,
    c_int,
]
C_LIB.get_fct_mmf.restype = FCTStruct

C_LIB.free_fctstruct = C_LIB.free_fctstruct
C_LIB.free_fctstruct.argtypes = [FCTStruct]
C_LIB.free_fctstruct.restype = None


def fix_seed(seed):
    np.random.seed(seed)


parser = argparse.ArgumentParser(description="")
parser.add_argument("--shard", dest="shard", type=int, default=0, help="random seed")
# parser.add_argument("--cc", dest="cc", action="store", default="dctcp", help="")
parser.add_argument(
    "--nhost", dest="nhost", type=int, default=1, help="number of hosts"
)
parser.add_argument(
    "-b",
    dest="bw",
    action="store",
    type=int,
    default=1,
    help="bandwidth of edge link (Gbps)",
)
parser.add_argument(
    "--root",
    dest="root",
    action="store",
    default="mix",
    help="the root directory for configs and results",
)
args = parser.parse_args()

fix_seed(args.shard)

dir_input = args.root

nhost = int(args.nhost)
bw = int(args.bw)
output_dir = dir_input
# output_dir = "/data1/lichenni/projects/flow_simulation/High-Precision-Congestion-Control/gc"
# os.makedirs(output_dir, exist_ok=True)
fcts_flowsim_path = f"{output_dir}/fcts_flowsim.npy"
print("fcts_flowsim_path: ", fcts_flowsim_path)
if not os.path.exists(fcts_flowsim_path) and os.path.exists(
    f"{dir_input}/flow_sizes.npy"
):
    sizes = np.load(f"{dir_input}/flow_sizes.npy")
    fats = np.load(f"{dir_input}/flow_arrival_times.npy")
    flow_src_dst = np.load(f"{dir_input}/flow_src_dst.npy")

    n_flows = len(sizes)
    MTU = 1000
    HEADER_SIZE = 48

    start = time()
    fats_pt = make_array(c_double, fats)
    sizes_pt = make_array(c_double, sizes)
    # n_links_passed = abs(flow_src_dst[:, 0] - flow_src_dst[:, 1]) + 2
    # pkt_head = np.clip(sizes, a_min=0, a_max=MTU)
    # pkt_rest = sizes - pkt_head
    # size_byte = (
    #     (pkt_head + HEADER_SIZE) * n_links_passed
    #     + (pkt_rest + np.ceil(pkt_rest / MTU) * HEADER_SIZE)
    # ).astype("int64")
    # sizes_pt = make_array(c_double, size_byte)
    src_pt = make_array(c_int, flow_src_dst[:, 0])
    dst_pt = make_array(c_int, flow_src_dst[:, 1])
    # topo_pt=make_array(c_int, np.array([2,2,1,2,1,1]))
    topo_pt = make_array(c_int, np.array([1, 4]))
    res = C_LIB.get_fct_mmf(
        n_flows, fats_pt, sizes_pt, src_pt, dst_pt, nhost, topo_pt, 1, 8, 2, bw
    )

    estimated_fcts = np.fromiter(res.estimated_fcts, dtype=np.float64, count=n_flows)

    # t_flows = np.fromiter(res.t_flows, dtype=np.float64, count=2 * n_flows)
    # num_flows = np.fromiter(res.num_flows, dtype=np.uint, count=2 * n_flows).astype(
    # np.int64
    # )
    # num_flows_enq = np.fromiter(res.num_flows_enq, dtype=np.uint, count=n_flows).astype(
    #     np.int64
    # )
    end = time()
    print("c_sim:%f" % (end - start))
    print("estimated_fcts:%f" % (np.mean(estimated_fcts)))
    # print(f"t_flows-{len(t_flows)}: {np.mean(t_flows)}")
    # print(f"num_flows-{len(num_flows)}: {np.mean(num_flows)}")
    # print(f"num_flows_enq-{len(num_flows_enq)}: {np.mean(num_flows_enq)}")

    np.save("%s/fcts_flowsim.npy" % output_dir, estimated_fcts)
    os.system("rm %s/traffic.txt" % (output_dir))
    # np.save(f"{output_dir}/t_flows_flowsim.npy", np.array(t_flows))
    # np.save(f"{output_dir}/num_flows_flowsim.npy", np.array(num_flows))
    # np.save(f"{output_dir}/num_flows_enq_flowsim.npy", num_flows_enq)
    C_LIB.free_fctstruct(res)
