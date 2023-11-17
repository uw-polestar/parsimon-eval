use std::{ops::AddAssign, path::Path};

use crate::fabric::Cluster;
use parsimon::core::network::NodeId;
use rand::{prelude::*, Rng};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::entry::Entry;

const NR_TORS_PER_POD: usize = 48;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SpatialData {
    pub matrix: Tor2TorMatrix,
    pub pod2tors: FxHashMap<String, Vec<String>>,
    pub nr_pods: usize,
    pub nr_racks: usize,
}

impl SpatialData {
    pub fn from_csv(path: impl AsRef<Path>) -> Result<Self, Error> {
        let mut rdr = csv::Reader::from_path(&path)?;
        let mut pod2tors = FxHashMap::default();
        let mut rack2rack2count = FxHashMap::default();
        for result in rdr.deserialize() {
            let entry: Entry = result?;
            pod2tors
                .entry(entry.srcpod)
                .or_insert(FxHashSet::default())
                .insert(entry.srcrack.clone());
            pod2tors
                .entry(entry.dstpod)
                .or_insert(FxHashSet::default())
                .insert(entry.dstrack.clone());
            rack2rack2count
                .entry(entry.srcrack)
                .or_insert(FxHashMap::default())
                .entry(entry.dstrack)
                .or_insert(0_usize)
                .add_assign(1);
        }
        for tors in pod2tors.values() {
            if tors.len() > NR_TORS_PER_POD {
                return Err(Error::TooManyRacks);
            }
        }
        let racks = pod2tors
            .values()
            .flat_map(|rack| rack.iter())
            .collect::<Vec<_>>();

        // Now each rack is given an index in the matrix
        let nr_max_tors = pod2tors.len() * NR_TORS_PER_POD;
        let (idx2name, name2idx): (Vec<_>, FxHashMap<_, _>) = racks
            .iter()
            .enumerate()
            .map(|(i, &name)| (name.clone(), (name, i)))
            .unzip();

        // Construct the matrix
        let mut inner = vec![vec![0; nr_max_tors]; nr_max_tors];
        for (src, dsts) in rack2rack2count {
            let src = *name2idx.get(&src).unwrap();
            for (dst, count) in dsts {
                let dst = *name2idx.get(&dst).unwrap();
                inner[src][dst] += count;
            }
        }
        let matrix = Tor2TorMatrix::new(inner, idx2name);
        let nr_pods = pod2tors.len();
        let nr_racks = racks.len();
        let pod2tors = pod2tors
            .into_iter()
            .map(|(pod, tors)| (pod, tors.into_iter().collect()))
            .collect();
        Ok(Self {
            matrix,
            pod2tors,
            nr_pods,
            nr_racks,
        })
    }

    pub fn map_to(&self, cluster: &Cluster, mut rng: impl Rng) -> Result<SpatialWorkload, Error> {
        // Collect matrix info
        let dim = self.matrix.dim();
        let cumsum = self
            .matrix
            .inner
            .iter()
            .flat_map(|row| row.iter())
            .scan(0, |acc, x| {
                *acc += x;
                Some(*acc)
            })
            .collect();

        // Map the matrix onto the cluster. Pods can be placed in an arbitrary order. Give each
        // pod hash an arbitrary index in `cluster.pods`.
        if self.pod2tors.len() != cluster.pods.len() {
            return Err(Error::WorkloadClusterMismatch);
        }
        let pod2idx = self
            .pod2tors
            .keys()
            .cloned()
            .enumerate()
            .map(|(i, pod)| (pod, i))
            .collect::<FxHashMap<_, _>>();
        // Now within a pod, a ToR hash is randomly assigned to a ToR node.
        let name2tor = self
            .pod2tors
            .iter()
            .flat_map(|(pod, tors)| {
                let pod_idx = *pod2idx.get(pod).unwrap();
                let mut tor_ids = cluster.pods[pod_idx]
                    .racks
                    .iter()
                    .map(|rack| rack.tor.id)
                    .collect::<Vec<_>>();
                tor_ids.shuffle(&mut rng);
                tors.iter().zip(tor_ids)
            })
            .collect::<FxHashMap<_, _>>();

        // Get a list of host IDs for each ToR ID.
        let tor2hosts = cluster
            .pods
            .iter()
            .flat_map(|p| p.racks.iter())
            .map(|r| {
                let host_ids = r.hosts.iter().map(|h| h.id).collect::<Vec<_>>();
                (r.tor.id, host_ids)
            })
            .collect::<FxHashMap<_, _>>();

        // Now chain the `idx2name`, `name2tor`, and `tor2hosts` maps to get an `idx2hosts` map.
        let idx2hosts = self
            .matrix
            .idx2name
            .iter()
            .map(|name| {
                let tor = name2tor.get(name).unwrap();
                let hosts = tor2hosts.get(tor).unwrap().clone();
                hosts
            })
            .collect::<Vec<_>>();

        Ok(SpatialWorkload {
            dim,
            cumsum,
            idx2hosts,
        })
    }

    pub fn downsample(&self, nr_pods: usize, nr_tors_per_pod: usize, mut rng: impl Rng) -> Self {
        // Choose which racks to keep
        let new_pod2tors = self
            .pod2tors
            .keys()
            .cloned()
            .choose_multiple(&mut rng, nr_pods)
            .into_iter()
            .map(|pod| {
                let tors = self
                    .pod2tors
                    .get(&pod)
                    .unwrap()
                    .choose_multiple(&mut rng, nr_tors_per_pod)
                    .cloned()
                    .collect::<Vec<_>>();
                (pod, tors)
            })
            .collect::<FxHashMap<_, _>>();
        let tors = new_pod2tors
            .values()
            .flat_map(|tors| tors.iter())
            .collect::<FxHashSet<_>>();
        assert_eq!(tors.len(), nr_pods * nr_tors_per_pod);

        // Find the matrix indices of those pods
        let keep_indices = self
            .matrix
            .idx2name
            .iter()
            .enumerate()
            .filter_map(|(i, name)| tors.contains(name).then_some(i))
            .collect::<FxHashSet<_>>();

        // Construct a new `Tor2TorMatrix`
        let new_inner = self
            .matrix
            .inner
            .iter()
            .enumerate()
            .filter(|&(i, _)| keep_indices.contains(&i))
            .map(|(_, row)| {
                row.iter()
                    .enumerate()
                    .filter_map(|(i, &count)| keep_indices.contains(&i).then_some(count))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let new_idx2name = keep_indices
            .iter()
            .map(|&i| self.matrix.idx2name[i].clone())
            .collect::<Vec<_>>();
        let new_matrix = Tor2TorMatrix::new(new_inner, new_idx2name);
        let nr_racks = tors.len();
        Self {
            matrix: new_matrix,
            pod2tors: new_pod2tors,
            nr_pods,
            nr_racks,
        }
    }
}

#[derive(Debug, derive_new::new, serde::Serialize, serde::Deserialize)]
pub struct Tor2TorMatrix {
    pub inner: Vec<Vec<usize>>,
    pub idx2name: Vec<String>,
}

impl Tor2TorMatrix {
    pub fn dim(&self) -> usize {
        self.inner.len()
    }

    pub fn row_weights(&self) -> impl Iterator<Item = f64> + '_ {
        let sum = self.inner_sum();
        self.inner
            .iter()
            .map(move |v| v.iter().sum::<usize>() as f64 / sum as f64)
    }

    pub fn col_weights(&self) -> impl Iterator<Item = f64> + '_ {
        let sum = self.inner_sum();
        let n = self.dim();
        (0..n).map(move |i| self.inner.iter().map(|v| v[i]).sum::<usize>() as f64 / sum as f64)
    }

    pub fn diag_weight(&self) -> f64 {
        let sum = self.inner_sum();
        let n = self.dim();
        (0..n).map(|i| self.inner[i][i]).sum::<usize>() as f64 / sum as f64
    }

    pub fn off_diag_weight(&self) -> f64 {
        1.0 - self.diag_weight()
    }

    fn inner_sum(&self) -> usize {
        self.inner.iter().flat_map(|v| v.iter()).sum()
    }
}

#[derive(Debug)]
pub struct SpatialWorkload {
    dim: usize,
    cumsum: Vec<usize>,
    // Maps an index in the original `Tor2TorMatrix` to a list of host IDs
    idx2hosts: Vec<Vec<NodeId>>,
}

impl SpatialWorkload {
    pub fn sample(&self, mut rng: impl Rng) -> (NodeId, NodeId) {
        let tot = *self.cumsum.last().unwrap();
        let random = rng.gen_range(0..tot);
        let index = match self.cumsum.binary_search(&random) {
            Ok(i) => {
                // `random` was found at `i`
                let mut index = i;
                while random >= self.cumsum[index] {
                    index += 1;
                }
                index
            }
            Err(i) => {
                // `random` could be placed at `i`
                i
            }
        };
        let (src_idx, dst_idx) = (index / self.dim, index % self.dim);
        let (src_choices, dst_choices) = (&self.idx2hosts[src_idx], &self.idx2hosts[dst_idx]);
        let mut src_host = NodeId::new(0);
        let mut dst_host = NodeId::new(0);
        while src_host == dst_host {
            src_host = *src_choices.choose(&mut rng).unwrap();
            dst_host = *dst_choices.choose(&mut rng).unwrap();
        }
        (src_host, dst_host)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("too many racks for the number of pods in the dataset")]
    TooManyRacks,

    #[error("cannot map spatial workload to cluster")]
    WorkloadClusterMismatch,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Csv(#[from] csv::Error),
}
